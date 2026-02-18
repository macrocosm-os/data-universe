import json
import math
import random
import statistics
import asyncio
import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import bittensor as bt
from common.data import DataSource, DataLabel, DataEntity
from common.constants import X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE
from common import constants
from scraping.provider import ScraperProvider
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.x.model import XContent
from scraping.reddit.model import RedditContent
from vali_utils.on_demand import utils as on_demand_utils
from vali_utils.on_demand.output_models import validate_metadata_completeness
from vali_utils.metrics import ORGANIC_MINER_RESULTS


@dataclass
class ValidationContext:
    """Lightweight replacement for OrganicRequest (bt.Synapse) used only for validation."""

    source: str
    usernames: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    url: Optional[str] = None
    keyword_mode: str = "all"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: Optional[int] = 100


class OnDemandValidator:
    """Stateless validation utility for on-demand job submissions.

    Extracted from the legacy OrganicQueryProcessor — contains only the
    validation / scoring helpers that ``neurons/validator.py`` needs for
    the API-based job flow.  No dendrite / axon dependency.
    """

    PER_MINER_VALIDATION_SAMPLE_SIZE = 2
    MIN_CONSENSUS = 0.3

    def __init__(self, metagraph: bt.metagraph, evaluator):
        self.metagraph = metagraph
        self.evaluator = evaluator

    def update_metagraph(self, metagraph: bt.metagraph):
        self.metagraph = metagraph

    # ------------------------------------------------------------------
    # Format validation
    # ------------------------------------------------------------------

    def _validate_miner_data_format(
        self, ctx: ValidationContext, data: List, miner_uid: int
    ) -> bool:
        if not data:
            return True

        source = ctx.source.upper()
        seen_post_ids = set()

        for i, item in enumerate(data):
            try:
                if not isinstance(item, DataEntity):
                    bt.logging.debug(
                        f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]}: Item {i} is not a DataEntity"
                    )
                    return False

                if not item.uri or not item.content:
                    bt.logging.debug(
                        f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]}: Item {i} missing required fields (uri, content)"
                    )
                    return False

                post_id = self._get_post_id(item)
                if post_id in seen_post_ids:
                    bt.logging.info(
                        f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} has duplicate posts in response (item {i}): {post_id}"
                    )
                    return False
                seen_post_ids.add(post_id)

                if source == "X":
                    x_content = XContent.from_data_entity(item)
                    if (
                        on_demand_utils.is_nested_format(item)
                        and dt.datetime.now(tz=dt.timezone.utc)
                        < X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE
                    ):
                        item = XContent.to_data_entity(x_content)
                elif source == "REDDIT":
                    RedditContent.from_data_entity(item)

            except Exception as e:
                bt.logging.info(
                    f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} format validation failed on item {i}: {str(e)}"
                )
                return False

        bt.logging.trace(
            f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]}: Successfully validated all {len(data)} items for formatting and duplicates."
        )
        return True

    # ------------------------------------------------------------------
    # Post ID helper
    # ------------------------------------------------------------------

    def _get_post_id(self, post) -> str:
        if isinstance(post, dict):
            return post.get("uri") or str(hash(str(sorted(post.items()))))
        return getattr(post, "uri", None) or str(hash(str(post)))

    # ------------------------------------------------------------------
    # Entity validation (three-phase)
    # ------------------------------------------------------------------

    async def _validate_entity(
        self,
        ctx: ValidationContext,
        entity: DataEntity,
        post_id: str,
        miner_uid: int,
    ) -> bool:
        try:
            # Phase 1: Request field validation
            if not self._validate_request_fields(ctx, entity, miner_uid):
                bt.logging.error(
                    f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} post {post_id} failed request field validation"
                )
                return False

            # Phase 2: Metadata completeness
            if not self._validate_metadata_completeness(entity, post_id, miner_uid):
                bt.logging.error(
                    f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} post {post_id} failed metadata completeness validation"
                )
                return False

            # Convert to proper format for scraper validation if needed
            if ctx.source.upper() == "X" and on_demand_utils.is_nested_format(entity):
                x_content = XContent.from_data_entity(entity)
                entity_for_validation = XContent.to_data_entity(content=x_content)
            else:
                entity_for_validation = entity

            # Phase 3: Scraper validation
            return await self._validate_with_scraper(
                ctx, entity_for_validation, post_id
            )

        except Exception as e:
            bt.logging.error(
                f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} validation error for {post_id}: {str(e)}"
            )
            return False

    # ------------------------------------------------------------------
    # Request-field validation
    # ------------------------------------------------------------------

    def _validate_request_fields(
        self, ctx: ValidationContext, entity: DataEntity, miner_uid: int
    ) -> bool:
        try:
            if ctx.source.upper() == "X":
                return self._validate_x_request_fields(ctx, entity, miner_uid)
            elif ctx.source.upper() == "REDDIT":
                return self._validate_reddit_request_fields(ctx, entity, miner_uid)
        except Exception as e:
            bt.logging.error(f"Error in request field validation: {str(e)}")
            return False

    def _validate_x_request_fields(
        self, ctx: ValidationContext, x_entity: DataEntity, miner_uid: int
    ) -> bool:
        hotkey = self.metagraph.hotkeys[miner_uid]
        x_content_dict = json.loads(x_entity.content.decode("utf-8"))

        if ctx.url:
            from scraping.x import utils

            post_url = x_content_dict.get("url", "")
            if utils.normalize_url(post_url) != utils.normalize_url(ctx.url):
                bt.logging.debug(
                    f"Miner {miner_uid}:{hotkey} URL mismatch: {post_url} != {ctx.url}"
                )
                return False
            if not self._validate_time_range(ctx, x_entity.datetime):
                bt.logging.debug(
                    f"Miner {miner_uid}:{hotkey} failed time range validation"
                )
                return False
            return True

        if ctx.usernames:
            requested_usernames = [u.strip("@").lower() for u in ctx.usernames]
            if "user" in x_content_dict:
                user_dict = x_content_dict.get("user", {})
                post_username = user_dict.get("username", "").strip("@").lower()
            else:
                post_username = x_content_dict.get("username", "").strip("@").lower()

            if not post_username or post_username not in requested_usernames:
                bt.logging.debug(
                    f"Miner {miner_uid}:{hotkey} username mismatch: {post_username} not in: {requested_usernames}"
                )
                return False

        if ctx.keywords:
            post_text = x_content_dict.get("text", "").lower()
            keyword_mode = ctx.keyword_mode
            if keyword_mode == "all":
                if not all(
                    keyword.lower() in post_text for keyword in ctx.keywords
                ):
                    bt.logging.debug(
                        f"Miner {miner_uid}:{hotkey} not all keywords ({ctx.keywords}) found in post: {post_text}"
                    )
                    return False
            else:
                if not any(
                    keyword.lower() in post_text for keyword in ctx.keywords
                ):
                    bt.logging.debug(
                        f"Miner {miner_uid}:{hotkey} none of the keywords ({ctx.keywords}) found in post: {post_text}"
                    )
                    return False

        if not self._validate_time_range(ctx, x_entity.datetime):
            bt.logging.debug(
                f"Miner {miner_uid}:{hotkey} failed time range validation"
            )
            return False

        return True

    def _validate_reddit_request_fields(
        self, ctx: ValidationContext, reddit_entity: DataEntity, miner_uid: int
    ) -> bool:
        hotkey = self.metagraph.hotkeys[miner_uid]
        bt.logging.debug(
            f"Miner {miner_uid}:{hotkey} - Starting Reddit request field validation"
        )
        reddit_content_dict = json.loads(reddit_entity.content.decode("utf-8"))

        if ctx.usernames:
            requested_usernames = [u.lower() for u in ctx.usernames]
            post_username = reddit_content_dict.get("username")
            if (
                not post_username
                or post_username.lower() not in requested_usernames
            ):
                bt.logging.debug(
                    f"Miner {miner_uid}:{hotkey} Reddit username mismatch: {post_username} not in: {requested_usernames}"
                )
                return False

        if ctx.keywords:
            post_community = reddit_content_dict.get("communityName")
            if post_community:
                post_community = post_community.lower().removeprefix("r/")
                subreddit_match = any(
                    keyword.lower().removeprefix("r/") == post_community
                    for keyword in ctx.keywords
                )
            else:
                subreddit_match = False

            body_text = reddit_content_dict.get("body") or ""
            title_text = reddit_content_dict.get("title") or ""
            content_text = (body_text + " " + title_text).lower().strip()

            keyword_mode = ctx.keyword_mode
            if keyword_mode == "all":
                keyword_in_content = (
                    all(
                        keyword.lower() in content_text
                        for keyword in ctx.keywords
                    )
                    if content_text
                    else False
                )
            else:
                keyword_in_content = (
                    any(
                        keyword.lower() in content_text
                        for keyword in ctx.keywords
                    )
                    if content_text
                    else False
                )

            if not (subreddit_match or keyword_in_content):
                bt.logging.debug(
                    f"Miner {miner_uid}:{hotkey} Reddit keyword mismatch in subreddit: '{post_community}' and content: '{content_text}'"
                )
                return False

        if not self._validate_time_range(ctx, reddit_entity.datetime):
            bt.logging.debug(
                f"Miner {miner_uid}:{hotkey} failed time range validation"
            )
            return False

        return True

    # ------------------------------------------------------------------
    # Time range validation
    # ------------------------------------------------------------------

    def _validate_time_range(
        self, ctx: ValidationContext, post_timestamp: dt.datetime
    ) -> bool:
        from common import utils

        try:
            if ctx.start_date:
                start_dt = utils.parse_iso_date(ctx.start_date)
                if post_timestamp < start_dt:
                    bt.logging.debug(
                        f"Post timestamp {post_timestamp} is before start date {start_dt}"
                    )
                    return False
            if ctx.end_date:
                end_dt = utils.parse_iso_date(ctx.end_date)
                if post_timestamp > end_dt:
                    bt.logging.debug(
                        f"Post timestamp {post_timestamp} is after end date {end_dt}"
                    )
                    return False
            return True
        except Exception as e:
            bt.logging.error(f"Error validating time range: {str(e)}")
            return False

    # ------------------------------------------------------------------
    # Metadata completeness
    # ------------------------------------------------------------------

    def _validate_metadata_completeness(
        self, entity: DataEntity, post_id: str = None, miner_uid: int = None
    ) -> bool:
        try:
            is_valid, missing_fields = validate_metadata_completeness(entity)
            if not is_valid:
                source = DataSource(entity.source).name.upper()
                post_identifier = post_id or entity.uri or "unknown"
                bt.logging.info(
                    f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} - {source} post {post_identifier} missing required metadata: {missing_fields}"
                )
                return False
            return True
        except Exception as e:
            bt.logging.error(
                f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} - Error validating metadata completeness: {str(e)}"
            )
            return False

    # ------------------------------------------------------------------
    # Scraper validation
    # ------------------------------------------------------------------

    async def _validate_with_scraper(
        self, ctx: ValidationContext, data_entity: DataEntity, post_id: str
    ) -> bool:
        try:
            scraper = self._get_scraper(ctx.source)
            if not scraper:
                bt.logging.warning(f"No scraper available for {ctx.source}")
                return False

            if ctx.source.upper() == "X":
                results = await scraper.validate(
                    entities=[data_entity], allow_low_engagement=True
                )
            else:
                results = await scraper.validate([data_entity])

            if results and len(results) > 0:
                result = results[0]
                is_valid = (
                    result.is_valid if hasattr(result, "is_valid") else bool(result)
                )
                if not is_valid:
                    bt.logging.error(
                        f"Post {post_id} failed scraper validation: {getattr(result, 'reason', 'Unknown')}"
                    )
                return is_valid
            else:
                bt.logging.error(f"No scraper validation results for {post_id}")
                return False

        except Exception as e:
            bt.logging.error(
                f"Scraper validation error for {post_id}: {str(e)}"
            )
            return False

    def _get_scraper(self, source: str):
        try:
            if source.upper() == "X":
                return ApiDojoTwitterScraper()
            else:
                scraper_id = self.evaluator.PREFERRED_SCRAPERS.get(
                    DataSource[source.upper()]
                )
                if scraper_id:
                    return ScraperProvider().get(scraper_id)
        except Exception as e:
            bt.logging.error(f"Error getting scraper for {source}: {str(e)}")
        return None

    # ------------------------------------------------------------------
    # Validation penalties
    # ------------------------------------------------------------------

    def _apply_validation_penalties(
        self,
        miner_responses: Dict[int, List],
        validation_results: Dict[str, bool],
    ) -> Tuple[Dict[int, int], List[int], List[int]]:
        miner_scores = {}
        failed_miners = []
        successful_miners = []

        for uid in miner_responses.keys():
            if not miner_responses[uid]:
                miner_scores[uid] = 0
                failed_miners.append(uid)
                continue

            miner_failed_validation = False
            for post in miner_responses[uid]:
                post_id = self._get_post_id(post)
                miner_post_key = f"{uid}:{post_id}"
                if miner_post_key in validation_results:
                    if not validation_results[miner_post_key]:
                        miner_failed_validation = True
                        bt.logging.error(
                            f"Miner {uid}:{self.metagraph.hotkeys[uid]} failed validation for post {post_id}"
                        )
                        break

            if miner_failed_validation:
                miner_scores[uid] = 0
                failed_miners.append(uid)
                self.evaluator.scorer.apply_ondemand_penalty(
                    uid=uid, mult_factor=1.0
                )
                ORGANIC_MINER_RESULTS.labels(
                    miner_uid=uid, result_type="failure_content_validation"
                ).inc()
            else:
                successful_miners.append(uid)
                ORGANIC_MINER_RESULTS.labels(
                    miner_uid=uid, result_type="success"
                ).inc()

        bt.logging.info(f"Final miner scores: {miner_scores}")
        bt.logging.info(f"Failed validation miners: {failed_miners}")
        bt.logging.info(f"Successful validation miners: {successful_miners}")
        return miner_scores, failed_miners, successful_miners

    # ------------------------------------------------------------------
    # Volume consensus
    # ------------------------------------------------------------------

    def _calculate_volume_consensus(
        self, miner_data_counts: Dict[int, int]
    ) -> Optional[float]:
        if not miner_data_counts or len(miner_data_counts) < 2:
            return None

        non_zero_counts = [
            count for count in miner_data_counts.values() if count > 0
        ]
        if len(non_zero_counts) < 2:
            return None

        median_count = statistics.median(non_zero_counts)
        mean_count = statistics.mean(non_zero_counts)
        consensus_count = max(median_count, mean_count)

        bt.logging.info(
            f"Volume consensus: {consensus_count:.1f} posts (median: {median_count}, mean: {mean_count:.1f})"
        )
        return consensus_count

    def _apply_consensus_volume_penalties(
        self,
        miner_data_counts: Dict[int, int],
        requested_limit: Optional[int],
        consensus_count: Optional[float],
    ) -> List[int]:
        if consensus_count is None:
            bt.logging.info(
                "Not enough miners with data for consensus - skipping volume penalties"
            )
            return []

        if requested_limit is not None:
            min_consensus_threshold = requested_limit * self.MIN_CONSENSUS
            if consensus_count < min_consensus_threshold:
                bt.logging.info(
                    "Consensus shows limited data available - skipping volume penalties"
                )
                return []

        penalized_miners = []

        for uid, post_count in miner_data_counts.items():
            if post_count > 0 and post_count < consensus_count:
                underperformance_ratio = 1.0 - (post_count / consensus_count)
                if underperformance_ratio > 0.2:
                    mult_factor = min(
                        (underperformance_ratio - 0.2) / 0.8, 1.0
                    )
                    penalized_miners.append(uid)
                    bt.logging.info(
                        f"Miner {uid}:{self.metagraph.hotkeys[uid]}: {post_count} posts vs consensus {consensus_count:.1f} "
                        f"({underperformance_ratio:.1%} underperformance, {mult_factor:.2f} penalty)"
                    )
                    self.evaluator.scorer.apply_ondemand_penalty(
                        uid=uid, mult_factor=mult_factor
                    )
                    ORGANIC_MINER_RESULTS.labels(
                        miner_uid=uid,
                        result_type="failure_volume_underperformance_consensus",
                    ).inc()

        bt.logging.info(
            f"Applied consensus volume penalties to {len(penalized_miners)} miners"
        )
        return penalized_miners

    # ------------------------------------------------------------------
    # Reward multipliers
    # ------------------------------------------------------------------

    def calculate_ondemand_reward_multipliers(
        self,
        job_created_at: dt.datetime,
        submission_timestamp: dt.datetime,
        returned_count: int,
        requested_limit: Optional[int],
        consensus_count: Optional[float] = None,
    ) -> Tuple[float, float]:
        # Speed multiplier (exponential decay)
        if submission_timestamp is None or job_created_at is None:
            bt.logging.warning(
                "Missing timestamp data for reward calculation, using default speed=0.5"
            )
            speed_multiplier = 0.5
        else:
            upload_time_seconds = (
                submission_timestamp - job_created_at
            ).total_seconds()
            decay_rate = 0.05
            speed_multiplier = max(
                0.1, math.exp(-decay_rate * upload_time_seconds)
            )
            bt.logging.trace(
                f"Upload time: {upload_time_seconds:.0f}s "
                f"-> speed multiplier (exponential): {speed_multiplier:.3f}"
            )

        # Volume multiplier
        if requested_limit is None:
            if consensus_count and consensus_count > 0:
                volume_multiplier = min(1.0, returned_count / consensus_count)
                bt.logging.trace(
                    f"Returned {returned_count} rows vs consensus {consensus_count:.0f} (no limit) "
                    f"-> volume multiplier: {volume_multiplier:.3f}"
                )
            else:
                volume_multiplier = 1.0
                bt.logging.trace(
                    f"Returned {returned_count} rows (no limit, no consensus) "
                    f"-> volume multiplier: {volume_multiplier:.3f}"
                )
        elif returned_count == 0:
            volume_multiplier = 0.0
            bt.logging.trace(
                f"Returned {returned_count}/{requested_limit} rows "
                f"-> volume multiplier: {volume_multiplier:.3f}"
            )
        else:
            volume_multiplier = min(1.0, returned_count / requested_limit)
            bt.logging.trace(
                f"Returned {returned_count}/{requested_limit} rows "
                f"-> volume multiplier: {volume_multiplier:.3f}"
            )

        return speed_multiplier, volume_multiplier
