import time
from typing import Any, Optional

import requests

from config import (
    API_BASE, CHANNEL_SLUG, INTERNAL_CHANNEL_URL,
    KICK_CLIENT_ID, KICK_CLIENT_SECRET, OAUTH_BASE,
)

try:
    from curl_cffi import requests as curl_requests
except Exception:
    curl_requests = None


class KickAPI:
    def __init__(self, slug: str | None = None):
        self.slug = (slug or CHANNEL_SLUG).strip().lower()
        self.token: Optional[str] = None
        self.token_expires_at = 0.0
        self.channel_cache: dict[str, Any] = {}
        self.last_channel_fetch = 0.0

    def _get_app_token(self) -> Optional[str]:
        if not KICK_CLIENT_ID or not KICK_CLIENT_SECRET:
            return None
        if self.token and time.time() < self.token_expires_at - 60:
            return self.token
        try:
            r = requests.post(
                f'{OAUTH_BASE}/oauth/token',
                data={
                    'grant_type': 'client_credentials',
                    'client_id': KICK_CLIENT_ID,
                    'client_secret': KICK_CLIENT_SECRET,
                },
                timeout=10,
            )
            r.raise_for_status()
            body = r.json()
            self.token = body.get('access_token')
            self.token_expires_at = time.time() + int(body.get('expires_in', 3600))
            return self.token
        except Exception as e:
            print(f'[KICK API] App token alınamadı: {e}')
            return None

    def get_channel_official(self) -> Optional[dict]:
        token = self._get_app_token()
        if not token or not self.slug:
            return None
        try:
            r = requests.get(
                f'{API_BASE}/public/v1/channels',
                params={'slug': self.slug},
                headers={'Authorization': f'Bearer {token}'},
                timeout=10,
            )
            if r.status_code == 401:
                self.token = None
                return None
            r.raise_for_status()
            data = r.json().get('data') or []
            return data[0] if data else None
        except Exception as e:
            print(f'[KICK API] channel lookup hatası: {e}')
            return None

    def get_channel_internal(self) -> Optional[dict]:
        if not self.slug:
            return None
        url = INTERNAL_CHANNEL_URL.format(slug=self.slug)
        try:
            if curl_requests:
                r = curl_requests.get(url, impersonate='chrome', timeout=10)
            else:
                r = requests.get(
                    url,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                     'AppleWebKit/537.36 (KHTML, like Gecko) '
                                     'Chrome/131.0 Safari/537.36'
                    },
                    timeout=10,
                )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f'[KICK WEB] channel lookup hatası: {e}')
            return None

    def resolve_channel(self, force=False) -> Optional[dict]:
        now = time.time()
        if not force and self.channel_cache and now - self.last_channel_fetch < 15:
            return self.channel_cache

        official = self.get_channel_official()
        internal = self.get_channel_internal()

        if not official and not internal:
            return self.channel_cache or None

        o = official or {}
        i = internal or {}
        stream = o.get('stream') or {}
        livestream = i.get('livestream') or {}
        user = i.get('user') or {}
        category = o.get('category') or livestream.get('category') or {}

        channel_id = (
            o.get('broadcaster_user_id') or i.get('id') or user.get('id')
        )
        chatroom_id = (
            i.get('chatroom', {}).get('id')
            or (i.get('chatroom') or {}).get('chatroom_id')
        )
        is_live = bool(
            stream.get('is_live')
            if 'is_live' in stream else livestream is not None
        )
        # Internal payload may contain an explicit livestream=None while offline.
        if 'livestream' in i:
            is_live = i.get('livestream') is not None

        result = {
            'slug': o.get('slug') or i.get('slug') or self.slug,
            'broadcaster_user_id': channel_id,
            'chatroom_id': chatroom_id,
            'title': o.get('stream_title') or livestream.get('session_title') or livestream.get('title'),
            'description': o.get('channel_description') or i.get('channel_description'),
            'is_live': is_live,
            'viewer_count': stream.get('viewer_count') if stream else livestream.get('viewer_count'),
            'started_at': stream.get('start_time') if stream else livestream.get('start_time'),
            'language': stream.get('language') if stream else livestream.get('language'),
            'thumbnail': stream.get('thumbnail') if stream else livestream.get('thumbnail'),
            'category_id': category.get('id'),
            'category_name': category.get('name'),
            'custom_tags': o.get('custom_tags') or livestream.get('custom_tags') or [],
            'active_subscribers_count': o.get('active_subscribers_count'),
            'active_gifted_subscribers_count': o.get('active_gifted_subscribers_count'),
            'raw_official': o,
            'raw_internal': i,
        }
        self.channel_cache = result
        self.last_channel_fetch = now
        return result
