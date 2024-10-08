from __future__ import print_function

import json
import re

import dateparser
import asyncio
import aiohttp
from yarl import URL

YOUTUBE_VIDEO_URL = 'https://www.youtube.com/watch?v={youtube_id}'
YOUTUBE_CONSENT_URL = 'https://consent.youtube.com/save'

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'

SORT_BY_POPULAR = 0
SORT_BY_RECENT = 1

YT_CFG_RE = r'ytcfg\.set\s*\(\s*({.+?})\s*\)\s*;'
YT_INITIAL_DATA_RE = r'(?:window\s*\[\s*["\']ytInitialData["\']\s*\]|ytInitialData)\s*=\s*({.+?})\s*;\s*(?:var\s+meta|</script|\n)'
YT_HIDDEN_INPUT_RE = r'<input\s+type="hidden"\s+name="([A-Za-z0-9_]+)"\s+value="([A-Za-z0-9_\-\.]*)"\s*(?:required|)\s*>'


class AsyncYoutubeCommentDownloader:

    def __init__(self, num_workers=None):
        self.num_workers = num_workers
        self.session_created = False

    async def create_session(self):
        if not self.session_created:
            self.session = aiohttp.ClientSession()
            self.session.headers.update({'User-Agent': USER_AGENT})
            cookie_jar = self.session.cookie_jar
            cookie_jar.update_cookies({'CONSENT': 'YES+cb'}, URL('.youtube.com'))
            self.session_created = True 

    async def _async_ajax_request(self, endpoint, ytcfg, retries=5, sleep=20, timeout=60):
        url = 'https://www.youtube.com' + endpoint['commandMetadata']['webCommandMetadata']['apiUrl']

        data = {'context': ytcfg['INNERTUBE_CONTEXT'],
                'continuation': endpoint['continuationCommand']['token']}
        
        for _ in range(retries):
            try:
                async with self.session.post(url, params={'key': ytcfg['INNERTUBE_API_KEY']}, json=data, timeout=timeout) as response:
                    if response.status == 200:
                        return await response.json()
                    if response.status in [403, 413]:
                        return {}
            except aiohttp.ClientTimeout:
                pass
            await asyncio.sleep(sleep)
    
    async def _worker(self, url_queue:asyncio.Queue, response_queue:asyncio.Queue, *args, **kwargs):
        while True:

            # get query
            url = await url_queue.get()

            # perform task 
            response_gen = await self._get_comments_from_url(url, *args, **kwargs)

            # store response 
            response_queue.put_nowait(response_gen)

            # Notify the queue that the query has been searched
            url_queue.task_done()

    async def _multiple_async_get_comments_from_url(self, urls:list|str, num_workers:int, *args, **kwargs):

        if isinstance(urls, str):
            urls = [urls]

        courutine:list[asyncio.Task] = []

        url_queue = asyncio.Queue()
        response_queue = asyncio.Queue()

        # putting queries in queue
        for url in urls:
            url_queue.put_nowait(url)

        # assigning jobs 
        for _ in range(num_workers):
            task = asyncio.create_task(self._worker(url_queue, response_queue, *args, **kwargs))
            courutine.append(task)

        await url_queue.join()

        # cancel the worker tasks
        for task in courutine:
            task.cancel()
        
        await asyncio.gather(*courutine, return_exceptions=True)

        # Retrieve results from the response queue
        results:list = []
        while not response_queue.empty():
            result = await response_queue.get()
            results.append(result)
        
        # close session
        await self.session.close()
        return results

    def async_get_comments(self, youtube_id_list:list|str, *args, **kwargs):

        if isinstance(youtube_id_list, list):
            urls = []
            for id in youtube_id_list:
                url = YOUTUBE_VIDEO_URL.format(youtube_id=id)
                urls.append(url)
        elif isinstance(youtube_id_list, str):
            urls = YOUTUBE_VIDEO_URL.format(youtube_id=youtube_id_list)


        comment_list = asyncio.run(self._multiple_async_get_comments_from_url(urls, self.num_workers, *args, **kwargs))

        return comment_list
        
    def async_get_comments_from_url(self, urls:list|str, sort_by=SORT_BY_RECENT, language=None, sleep=.1):
        comment_list = asyncio.run(self._multiple_async_get_comments_from_url(urls, self.num_workers, sort_by, language, sleep))
        return comment_list

    async def _get_comments_from_url(self, youtube_url, sort_by=SORT_BY_RECENT, language=None, sleep=.1):

        await self.create_session()

        async with self.session.get(youtube_url) as response:
            output = []
            if 'consent' in str(response.url):
                # We may get redirected to a separate page for cookie consent. If this happens we agree automatically.
                params = dict(re.findall(YT_HIDDEN_INPUT_RE, response.text))
                params.update({'continue': youtube_url, 'set_eom': False, 'set_ytc': True, 'set_apyt': True})
                response = self.session.post(YOUTUBE_CONSENT_URL, params=params)

            html = await response.text()
            ytcfg = json.loads(self.regex_search(html, YT_CFG_RE, default=''))
            if not ytcfg:
                return  # Unable to extract configuration
            if language:
                ytcfg['INNERTUBE_CONTEXT']['client']['hl'] = language

            data = json.loads(self.regex_search(html, YT_INITIAL_DATA_RE, default=''))

            item_section = next(self.search_dict(data, 'itemSectionRenderer'), None)
            renderer = next(self.search_dict(item_section, 'continuationItemRenderer'), None) if item_section else None
            if not renderer:
                # Comments disabled?
                return

            sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
            if not sort_menu:
                # No sort menu. Maybe this is a request for community posts?
                section_list = next(self.search_dict(data, 'sectionListRenderer'), {})
                continuations = list(self.search_dict(section_list, 'continuationEndpoint'))
                # Retry..
                data = await self._async_ajax_request(continuations[0], ytcfg) if continuations else {}
                sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
            if not sort_menu or sort_by >= len(sort_menu):
                raise RuntimeError('Failed to set sorting')
            continuations = [sort_menu[sort_by]['serviceEndpoint']]

            while continuations:
                continuation = continuations.pop()
                response = await self._async_ajax_request(continuation, ytcfg)

                if not response:
                    break

                error = next(self.search_dict(response, 'externalErrorMessage'), None)
                if error:
                    raise RuntimeError('Error returned from server: ' + error)

                actions = list(self.search_dict(response, 'reloadContinuationItemsCommand')) + \
                        list(self.search_dict(response, 'appendContinuationItemsAction'))
                for action in actions:
                    for item in action.get('continuationItems', []):
                        if action['targetId'] in ['comments-section',
                                                'engagement-panel-comments-section',
                                                'shorts-engagement-panel-comments-section']:
                            # Process continuations for comments and replies.
                            continuations[:0] = [ep for ep in self.search_dict(item, 'continuationEndpoint')]
                        if action['targetId'].startswith('comment-replies-item') and 'continuationItemRenderer' in item:
                            # Process the 'Show more replies' button
                            continuations.append(next(self.search_dict(item, 'buttonRenderer'))['command'])

                surface_payloads = self.search_dict(response, 'commentSurfaceEntityPayload')
                payments = {payload['key']: next(self.search_dict(payload, 'simpleText'), '')
                            for payload in surface_payloads if 'pdgCommentChip' in payload}
                if payments:
                    # We need to map the payload keys to the comment IDs.
                    view_models = [vm['commentViewModel'] for vm in self.search_dict(response, 'commentViewModel')]
                    surface_keys = {vm['commentSurfaceKey']: vm['commentId']
                                    for vm in view_models if 'commentSurfaceKey' in vm}
                    payments = {surface_keys[key]: payment for key, payment in payments.items() if key in surface_keys}

                toolbar_payloads = self.search_dict(response, 'engagementToolbarStateEntityPayload')
                toolbar_states = {payload['key']: payload for payload in toolbar_payloads}
                for comment in reversed(list(self.search_dict(response, 'commentEntityPayload'))):
                    properties = comment['properties']
                    cid = properties['commentId']
                    author = comment['author']
                    toolbar = comment['toolbar']
                    toolbar_state = toolbar_states[properties['toolbarStateKey']]
                    result = {'cid': cid,
                            'text': properties['content']['content'],
                            'time': properties['publishedTime'],
                            'author': author['displayName'],
                            'channel': author['channelId'],
                            'votes': toolbar['likeCountNotliked'].strip() or "0",
                            'replies': toolbar['replyCount'],
                            'photo': author['avatarThumbnailUrl'],
                            'heart': toolbar_state.get('heartState', '') == 'TOOLBAR_HEART_STATE_HEARTED',
                            'reply': '.' in cid}

                    try:
                        result['time_parsed'] = dateparser.parse(result['time'].split('(')[0].strip()).timestamp()
                    except AttributeError:
                        pass

                    if cid in payments:
                        result['paid'] = payments[cid]

                    output.append(result)
                await asyncio.sleep(sleep)
            return output

    @staticmethod
    def regex_search(text, pattern, group=1, default=None):
        match = re.search(pattern, text)
        return match.group(group) if match else default

    @staticmethod
    def search_dict(partial, search_key):
        stack = [partial]
        while stack:
            current_item = stack.pop()
            if isinstance(current_item, dict):
                for key, value in current_item.items():
                    if key == search_key:
                        yield value
                    else:
                        stack.append(value)
            elif isinstance(current_item, list):
                stack.extend(current_item)
