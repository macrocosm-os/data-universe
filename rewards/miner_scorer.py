import datetime as dt
import json
import threading
from typing import List, Optional

import bittensor as bt
import torch

from common.data import TimeBucket
from common.data_v2 import ScorableMinerIndex
from rewards.data_value_calculator import DataValueCalculator
from scraping.scraper import ValidationResult


class MinerScorer:
    """Tracks the score of each miner and handles updates to the scores.

    Thread safe.
    """

    # State version — bump this when saved state needs migration.
    # v1: Initial (no version key in state dict)
    # v2: Reset effective_sizes and s3_boosts to zero (exploit inflated sizes)
    # v3: Reset on-demand boosts/credibility (empty submissions were earning free credibility)
    # v4: Reset on-demand again after data existence probe + reddit body fix
    # v5: Full reset — engagement/uniqueness/URL checks reveal widespread exploit
    # v6: Reset S3 — .head() → .sample() fix (scraper sampling bypass)
    # v7: Reset OD — moved scoring to evaluator, dropped ^2.5 exponent, fixed credibility decay
    # v8: Reset S3 — strict schema check catches fabricated data; old inflated scores are invalid
    # v9: Reset OD — per-miner endpoint replaces poller; old boost/credibility based on broken lottery
    # v10: Reset S3 — per-file fabrication detection (user/dt diversity, templated text,
    #      hex-pad, synthetic users, short status IDs, within-file dup) catches the
    #      164-hotkey sybil cluster; pre-fix effective_size/credibility is inflated by
    #      fabricated bulk uploads.
    # v15: Reset S3 — latest-file-per-job validation + whole-job-snapshot miner uploads.
    #      Old effective_size/credibility was computed across all historical chunks; new
    #      model counts only the latest snapshot per job, so prior scores are not comparable.
    STATE_VERSION = 15

    # Start new miner's at a credibility of 0.
    STARTING_CREDIBILITY = 0

    # Start new miners' S3 credibility low — must earn it through passing validation
    STARTING_S3_CREDIBILITY = 0.1

    # The exponent used to scale the miner's score by its credibility.
    _CREDIBILITY_EXP = 2.5

    ONDEMAND_MAX_CRED_PENALTY = 0.0075  # 0.75%
    ONDEMAND_BASE_REWARD = 100_000_000  # 100M

    # On-demand credibility: tracks job participation rate via EMA.
    # Scales the entire final score — non-participants decay toward 0.
    STARTING_ONDEMAND_CREDIBILITY = 0.5
    ONDEMAND_CRED_ALPHA = 0.02  # EMA alpha: ~35 jobs (~70 sec) to halve
    ONDEMAND_CRED_BAD_DATA_PENALTY = 0.05  # 5% direct penalty per bad submission

    # P2P dampener — DD job weights (1.0-5.0) inflate P2P scores far beyond S3/OD.
    # DD jobs are designed for S3 uploads, not P2P index broadcasting.
    # This factor scales down the raw P2P score so it stays the smallest component.
    P2P_REWARD_SCALE = 0.05

    def __init__(
        self,
        num_neurons: int,
        value_calculator: DataValueCalculator,
        cred_alpha: float = 0.15,
        s3_cred_alpha: float = 0.30,
    ):
        # Tracks the raw scores of each miner. i.e. not the weights that are set on the blockchain.
        self.scores = torch.zeros(num_neurons, dtype=torch.float32)
        self.miner_credibility = torch.full((num_neurons, 1), MinerScorer.STARTING_CREDIBILITY, dtype=torch.float32)
        # Keeps track of the amount of scorable bytes the miner had last time it was evaluated.
        self.scorable_bytes = torch.zeros(num_neurons, dtype=torch.float32)
        self.value_calculator = value_calculator
        self.cred_alpha = cred_alpha

        # Keeps track of the miner's current S3 boost based on the last S3 evaluation.
        self.s3_boosts = torch.zeros(num_neurons, dtype=torch.float32)
        self.s3_credibility = torch.full((num_neurons, 1), MinerScorer.STARTING_S3_CREDIBILITY, dtype=torch.float32)
        self.s3_cred_alpha = s3_cred_alpha

        # Keeps track of the miner's OnDemand boost (EMA of recent OnDemand rewards)
        self.ondemand_boosts = torch.zeros(num_neurons, dtype=torch.float32)
        self.ondemand_alpha = 0.3

        # On-demand credibility: EMA of job participation (1=submitted, 0=skipped).
        # Scales entire final score — non-participants decay toward 0.
        self.ondemand_credibility = torch.full(
            (num_neurons, 1), MinerScorer.STARTING_ONDEMAND_CREDIBILITY, dtype=torch.float32
        )

        # Competition-based S3 scoring: stores effective_size for each miner
        # effective_size = total_size_bytes × coverage² (set during S3 validation)
        # S3 boost is calculated as: (my_effective_size / total_effective_size) × max_boost
        self.effective_sizes = torch.zeros(num_neurons, dtype=torch.float64)

        # Make this class thread safe because it'll eventually be accessed by multiple threads.
        # One from the main validator evaluation loop and another from a background thread performing validation on user requests.
        self.lock = threading.Lock()

    def save_state(self, filepath):
        """Save the current state to the provided filepath."""
        with self.lock:
            torch.save(
                {
                    "state_version": MinerScorer.STATE_VERSION,
                    "scores": self.scores,
                    "credibility": self.miner_credibility,
                    "s3_boosts": self.s3_boosts,
                    "s3_credibility": self.s3_credibility,
                    "scorable_bytes": self.scorable_bytes,
                    "ondemand_boosts": self.ondemand_boosts,
                    "ondemand_credibility": self.ondemand_credibility,
                    "effective_sizes": self.effective_sizes,
                },
                filepath,
            )

    def load_state(self, filepath):
        """Load the state from the provided filepath."""
        state = torch.load(filepath, weights_only=True)
        saved_version = state.get("state_version", 1)

        with self.lock:
            self.scores = state["scores"]
            self.miner_credibility = state["credibility"]
            self.s3_boosts = state.get("s3_boosts", torch.zeros_like(self.scores))
            self.s3_credibility = state.get(
                "s3_credibility",
                torch.full((self.scores.size(0), 1), MinerScorer.STARTING_S3_CREDIBILITY, dtype=torch.float32),
            )
            self.ondemand_boosts = state.get("ondemand_boosts", torch.zeros_like(self.scores))
            self.ondemand_credibility = state.get(
                "ondemand_credibility",
                torch.full((self.scores.size(0), 1), MinerScorer.STARTING_ONDEMAND_CREDIBILITY, dtype=torch.float32),
            )
            self.effective_sizes = state.get("effective_sizes", torch.zeros(self.scores.size(0), dtype=torch.float64))

            # --- State migrations ---
            if saved_version < 15:
                bt.logging.warning(
                    f"State migration v{saved_version} -> v15: "
                    f"S3 boost/credibility/effective_size reset "
                    f"(latest-file-per-job validation + whole-job-snapshot uploads)"
                )
                self.s3_boosts.zero_()
                self.s3_credibility.fill_(MinerScorer.STARTING_S3_CREDIBILITY)
                self.effective_sizes.zero_()

    def get_scores(self) -> torch.Tensor:
        """Returns the raw scores of all miners."""
        # Return a copy to ensure outside code can't modify the scores.
        with self.lock:
            return self.scores.clone()

    def get_scores_for_weights(self) -> torch.Tensor:
        """Returns scores with P2P capped at (S3 + OD) for weight setting."""
        with self.lock:
            capped_scores = torch.zeros_like(self.scores)

            for uid in range(len(self.scores)):
                p2p_cred = float(self.miner_credibility[uid] ** MinerScorer._CREDIBILITY_EXP)
                s3_cred = float(self.s3_credibility[uid] ** MinerScorer._CREDIBILITY_EXP)
                # OD: no exponent — ondemand_boosts EMA already decays non-participants.
                # The ^2.5 combined with broken per-job credibility polling was destroying
                # OD scores for all miners.
                od_cred = float(self.ondemand_credibility[uid])

                p2p_component = float(self.scorable_bytes[uid]) * MinerScorer.P2P_REWARD_SCALE * p2p_cred
                s3_component = float(self.s3_boosts[uid]) * s3_cred
                od_component = float(self.ondemand_boosts[uid]) * od_cred

                # S3 cap (existing): S3 <= 2x OD
                if od_component > 0:
                    s3_component = min(s3_component, od_component * 2)
                else:
                    s3_component = 0.0

                # P2P cap: P2P can't exceed S3 + OD
                service_component = s3_component + od_component
                p2p_capped = min(p2p_component, service_component) if service_component > 0 else 0.0

                capped_scores[uid] = p2p_capped + s3_component + od_component

            return capped_scores

    def get_credibilities(self) -> torch.Tensor:
        """Returns the raw credibilities of all miners."""
        # Return a copy to ensure outside code can't modify the scores.
        with self.lock:
            return self.miner_credibility.clone()

    def reset(self, uid: int) -> None:
        """Resets the score and credibility of miner 'uid'."""
        with self.lock:
            self.scores[uid] = 0.0
            self.miner_credibility[uid] = MinerScorer.STARTING_CREDIBILITY
            self.s3_boosts[uid] = 0.0
            self.s3_credibility[uid] = MinerScorer.STARTING_S3_CREDIBILITY
            self.ondemand_boosts[uid] = 0.0
            self.ondemand_credibility[uid] = MinerScorer.STARTING_ONDEMAND_CREDIBILITY
            self.effective_sizes[uid] = 0.0

    def get_miner_credibility(self, uid: int) -> float:
        """Returns the credibility of miner 'uid'."""
        with self.lock:
            return self.miner_credibility[uid].item()

    def resize(self, num_neurons: int) -> None:
        """Resizes the score tensor to the new number of neurons.

        The new size must be greater than or equal to the current size.
        """
        with self.lock:
            assert num_neurons >= self.scores.size(0), (
                f"Tried to downsize the number of neurons from {self.scores.size(0)} to {num_neurons}"
            )

            bt.logging.trace(f"Resizing MinerScorer from {self.scores.size(0)} to {num_neurons}")

            to_add = num_neurons - self.scores.size(0)
            self.scores = torch.cat([self.scores, torch.zeros(to_add, dtype=torch.float32)])
            self.miner_credibility = torch.cat(
                [
                    self.miner_credibility,
                    torch.full(
                        (to_add, 1),
                        MinerScorer.STARTING_CREDIBILITY,
                        dtype=torch.float32,
                    ),
                ]
            )
            self.scorable_bytes = torch.cat([self.scorable_bytes, torch.zeros(to_add, dtype=torch.float32)])

            self.s3_boosts = torch.cat([self.s3_boosts, torch.zeros(to_add, dtype=torch.float32)])
            self.s3_credibility = torch.cat(
                [
                    self.s3_credibility,
                    torch.full(
                        (to_add, 1),
                        MinerScorer.STARTING_S3_CREDIBILITY,
                        dtype=torch.float32,
                    ),
                ]
            )
            self.ondemand_boosts = torch.cat([self.ondemand_boosts, torch.zeros(to_add, dtype=torch.float32)])
            self.ondemand_credibility = torch.cat(
                [
                    self.ondemand_credibility,
                    torch.full(
                        (to_add, 1),
                        MinerScorer.STARTING_ONDEMAND_CREDIBILITY,
                        dtype=torch.float32,
                    ),
                ]
            )
            self.effective_sizes = torch.cat([self.effective_sizes, torch.zeros(to_add, dtype=torch.float64)])

    def update_s3_effective_size(
        self,
        uid: int,
        effective_size: float,
        validation_passed: bool,
    ) -> None:
        """
        Update miner's effective_size for S3 competition scoring.

        Similar to P2P scoring in sqlite_memory_validator_storage.py:
        scorable_bytes = my_bytes² / total_bytes

        For S3:
        s3_boost = (my_effective_size² / total_effective_size) × scale_factor × s3_credibility

        This rewards miners who have MORE data than others (squared advantage).

        FORGIVING APPROACH (like P2P credibility):
        - On success: Always update effective_size, increase credibility toward 1.0
        - On failure: KEEP previous effective_size, decrease credibility via EMA
        - This way one failed validation doesn't instantly zero out the miner
        - Consistent failures will eventually reduce credibility to near-zero

        Args:
            uid: Miner UID
            effective_size: total_size_bytes × coverage² (calculated from current validation)
            validation_passed: Whether the miner passed quality validation
        """
        with self.lock:
            old_effective = float(self.effective_sizes[uid])
            old_cred = float(self.s3_credibility[uid])

            # Update S3 credibility based on validation result
            if validation_passed:
                # Success: Update effective_size and increase credibility
                self.effective_sizes[uid] = effective_size
                # EMA toward 1.0: new_cred = alpha * 1.0 + (1-alpha) * old_cred
                self.s3_credibility[uid] = min(
                    1.0, self.s3_cred_alpha + (1 - self.s3_cred_alpha) * self.s3_credibility[uid]
                )
            else:
                # Failure: KEEP previous effective_size, reduce credibility via EMA
                # This is the forgiving part - one bad validation doesn't kill the miner
                # EMA toward 0: new_cred = alpha * 0.0 + (1-alpha) * old_cred = (1-alpha) * old_cred
                self.s3_credibility[uid] = (1 - self.s3_cred_alpha) * self.s3_credibility[uid]
                # Keep the old effective_size (don't update it)
                bt.logging.info(
                    f"S3 validation failed for miner {uid}: "
                    f"Keeping effective_size={old_effective / (1024 * 1024):.1f}MB, "
                    f"reducing credibility {old_cred:.4f} -> {float(self.s3_credibility[uid]):.4f}"
                )

            # Recalculate boosts for all miners (competition model)
            self._recalculate_s3_boosts_internal()

            new_cred = float(self.s3_credibility[uid])
            new_effective = float(self.effective_sizes[uid])

            bt.logging.info(
                f"S3 Update for miner {uid}: "
                f"effective_size={new_effective / (1024 * 1024):.1f}MB (was {old_effective / (1024 * 1024):.1f}MB), "
                f"s3_boost={float(self.s3_boosts[uid]):.0f}, "
                f"s3_cred={new_cred:.4f} (was {old_cred:.4f}), "
                f"passed={validation_passed}"
            )

    def _recalculate_s3_boosts_internal(self) -> None:
        """
        Internal method to recalculate S3 boosts using P2P-style competition.

        Formula (same as P2P sqlite storage):
        scorable_size = my_effective_size² / total_effective_size
        s3_boost = scorable_size × scale_factor

        This gives squared advantage to miners with more data.
        Example with 3 miners (100MB, 50MB, 50MB total = 200MB):
        - Miner A (100MB): 100² / 200 = 50MB scorable → 50M boost
        - Miner B (50MB):  50² / 200 = 12.5MB scorable → 12.5M boost
        - Miner C (50MB):  50² / 200 = 12.5MB scorable → 12.5M boost

        Requires: self.lock is held.
        """
        BYTES_TO_SCORE_SCALE = 1.0

        total_effective = float(self.effective_sizes.sum())

        if total_effective > 0:
            for uid in range(len(self.effective_sizes)):
                my_effective = float(self.effective_sizes[uid])
                if my_effective > 0:
                    # P2P-style: my_size² / total_size
                    scorable_size = (my_effective * my_effective) / total_effective
                    self.s3_boosts[uid] = scorable_size * BYTES_TO_SCORE_SCALE
                else:
                    self.s3_boosts[uid] = 0.0
        else:
            # No one has data
            for uid in range(len(self.effective_sizes)):
                self.s3_boosts[uid] = 0.0

    def recalculate_all_s3_boosts(self) -> None:
        """
        Recalculate S3 boosts for all miners based on current effective_sizes.

        Uses P2P-style competition: scorable_size = my_size² / total_size
        Call this before weight setting to ensure fair competition.
        """
        with self.lock:
            self._recalculate_s3_boosts_internal()

            total_effective = float(self.effective_sizes.sum())
            total_boosts = float(self.s3_boosts.sum())

            bt.logging.info(
                f"Recalculated S3 boosts for {len(self.effective_sizes)} miners. "
                f"Total effective_size: {total_effective / (1024 * 1024):.1f}MB, "
                f"Total s3_boosts: {total_boosts:.0f}"
            )

    def apply_ondemand_penalty(self, uid: int, mult_factor: float):
        """Applies an OD-only penalty: decays boost and OD credibility.

        Does NOT touch P2P credibility — OD failures should not contaminate
        P2P scoring. The composite score is recalculated after the update.
        """
        with self.lock:
            # EMA the ondemand boost with 0 reward for failures
            old_boost = float(self.ondemand_boosts[uid])
            self.ondemand_boosts[uid] = (1 - self.ondemand_alpha) * self.ondemand_boosts[uid]
            new_boost = float(self.ondemand_boosts[uid])

            # Direct penalty to on-demand credibility for bad data
            old_od_cred = float(self.ondemand_credibility[uid])
            self.ondemand_credibility[uid] = max(
                0, self.ondemand_credibility[uid] - MinerScorer.ONDEMAND_CRED_BAD_DATA_PENALTY * mult_factor
            )
            new_od_cred = float(self.ondemand_credibility[uid])

            bt.logging.info(
                json.dumps(
                    {
                        "event": "od_penalty",
                        "uid": uid,
                        "mult_factor": round(mult_factor, 4),
                        "od_cred_before": round(old_od_cred, 6),
                        "od_cred_after": round(new_od_cred, 6),
                        "boost_before": round(old_boost, 2),
                        "boost_after": round(new_boost, 2),
                    }
                )
            )

    def apply_ondemand_reward(self, uid: int, speed_multiplier: float, volume_multiplier: float):
        """
        Updates the miner's OnDemand boost based on their latest on-demand performance.
        The boost is EMA'd with alpha=0.3 and will be applied (scaled by credibility^2.5)
        during regular evaluation in on_miner_evaluated().

        Args:
            uid: Miner UID
            speed_multiplier: 0.1-1.0 based on upload speed (linear from 0-2 min)
            volume_multiplier: 0.0-1.0 based on rows_returned/limit
        """
        with self.lock:
            # Calculate raw reward for this on-demand job
            raw_reward = MinerScorer.ONDEMAND_BASE_REWARD * speed_multiplier * volume_multiplier

            # Update OnDemand boost using EMA (alpha=0.3)
            old_boost = float(self.ondemand_boosts[uid])
            self.ondemand_boosts[uid] = (
                self.ondemand_alpha * raw_reward + (1 - self.ondemand_alpha) * self.ondemand_boosts[uid]
            )
            new_boost = float(self.ondemand_boosts[uid])

            # Also bump OD credibility toward 1.0 on success (mirrors S3 credibility pattern)
            old_od_cred = float(self.ondemand_credibility[uid])
            alpha = MinerScorer.ONDEMAND_CRED_ALPHA
            self.ondemand_credibility[uid] = min(1.0, alpha * 1.0 + (1 - alpha) * self.ondemand_credibility[uid])
            new_od_cred = float(self.ondemand_credibility[uid])

            bt.logging.info(
                json.dumps(
                    {
                        "event": "od_reward",
                        "uid": uid,
                        "speed_mult": round(speed_multiplier, 4),
                        "volume_mult": round(volume_multiplier, 4),
                        "raw_reward": round(raw_reward, 2),
                        "boost_before": round(old_boost, 2),
                        "boost_after": round(new_boost, 2),
                        "od_cred_before": round(old_od_cred, 6),
                        "od_cred_after": round(new_od_cred, 6),
                    }
                )
            )

    def apply_ondemand_credibility_bump(self, uid: int, count: int = 1) -> None:
        """Small credibility bump for miners who submitted to OD jobs but weren't sampled.

        They participated (good signal) but we didn't verify their data,
        so we give a smaller credibility increase than a validated success.

        Args:
            uid: Miner UID.
            count: Number of unsampled submissions to apply (batched, single lock).
        """
        with self.lock:
            old_od_cred = float(self.ondemand_credibility[uid])
            alpha = MinerScorer.ONDEMAND_CRED_ALPHA * 0.5
            for _ in range(count):
                self.ondemand_credibility[uid] = min(1.0, alpha * 1.0 + (1 - alpha) * self.ondemand_credibility[uid])
            new_od_cred = float(self.ondemand_credibility[uid])

            bt.logging.trace(
                json.dumps(
                    {
                        "event": "od_credibility_bump",
                        "uid": uid,
                        "count": count,
                        "od_cred_before": round(old_od_cred, 6),
                        "od_cred_after": round(new_od_cred, 6),
                    }
                )
            )

    def on_miner_evaluated(
        self, uid: int, index: ScorableMinerIndex | None, validation_results: list[ValidationResult]
    ) -> None:
        """Notifies the scorer that a miner has been evaluated and should have its score updated.

        Args:
            uid (int): The miner's UID.
            index (ScorableMinerIndex): The latest index of the miner.
            validation_results (List[ValidationResult]): The results of data validation performed on the data provided by the miner.
        """
        with self.lock:
            score = 0.0

            # If the miner has an index, update it's credibility based on the validation result and score the current index.
            # Otherwise, score the miner 0 for this round, but don't touch its credibility.
            if index:
                # Compute the raw miner score based on the amount of data it has, scaled based on
                # the reward distribution.
                current_time_bucket = TimeBucket.from_datetime(dt.datetime.now(tz=dt.timezone.utc))
                for bucket in index.scorable_data_entity_buckets:
                    score += self.value_calculator.get_score_for_data_entity_bucket(bucket, current_time_bucket)

                # If the score has increased since the last eval, decrease credibility so that the
                # new score remains unchanged. i.e. "you've told us you now have more valuable data, prove it".
                # Note: After this step we then update the miner's credibility again, so if they passed
                # validation this time then their score will increase.
                previous_raw_score = self.scorable_bytes[uid].item()
                if previous_raw_score > 0 and score > previous_raw_score:
                    previous_cred = self.miner_credibility[uid].item()
                    cred_scalar = (previous_raw_score / score) ** (1 / MinerScorer._CREDIBILITY_EXP)
                    self.miner_credibility[uid] *= cred_scalar
                    bt.logging.debug(
                        f"Miner {uid}'s scorable bytes changed from {previous_raw_score} to {score}. Credibility changed from {previous_cred} to {self.miner_credibility[uid].item()}."
                    )

                # Record raw P2P score for next time.
                self.scorable_bytes[uid] = score

                # Update P2P credibility based on current validation results.
                self._update_credibility(uid, validation_results)

                # Independent component scoring — each channel gated by its own credibility.
                # This prevents one bad channel from zeroing out all rewards.
                p2p_cred = float(self.miner_credibility[uid] ** MinerScorer._CREDIBILITY_EXP)
                s3_cred = float(self.s3_credibility[uid] ** MinerScorer._CREDIBILITY_EXP)
                # OD: no exponent — the EMA-based credibility already decays non-participants.
                # The ^2.5 was destroying OD scores (near-zero values raised to 2.5 → effectively zero).
                od_cred = float(self.ondemand_credibility[uid])

                p2p_component = score * MinerScorer.P2P_REWARD_SCALE * p2p_cred
                s3_component = float(self.s3_boosts[uid]) * s3_cred
                od_component = float(self.ondemand_boosts[uid]) * od_cred

                # TEMPORARY: S3 capped at 2x on-demand component.
                # Prevents fabricators from profiting purely off S3 volume.
                # Legit miners with real OD participation still get rewarded for S3.
                # Will be replaced by proper incentive redesign.
                s3_uncapped = s3_component
                if od_component > 0:
                    s3_component = min(s3_component, od_component * 2)
                else:
                    s3_component = 0.0

                score = p2p_component + s3_component + od_component

                bt.logging.info(
                    f"Miner {uid} score breakdown: "
                    f"P2P={p2p_component:.0f} (raw={float(self.scorable_bytes[uid]):.0f}, cred={float(self.miner_credibility[uid]):.4f}), "
                    f"S3={s3_component:.0f} (boost={float(self.s3_boosts[uid]):.0f}, cred={float(self.s3_credibility[uid]):.4f})"
                    f"{f', capped from {s3_uncapped:.0f}' if s3_uncapped != s3_component else ''}, "
                    f"OD={od_component:.0f} (boost={float(self.ondemand_boosts[uid]):.0f}, cred={float(self.ondemand_credibility[uid]):.4f})"
                )

            self.scores[uid] = score

            bt.logging.success(
                f"Evaluated Miner {uid}. Score={self.scores[uid].item()}. "
                f"Credibility={self.miner_credibility[uid].item()}. "
                f"OnDemand Cred={float(self.ondemand_credibility[uid]):.4f}."
            )

    def _update_credibility(self, uid: int, validation_results: list[ValidationResult]):
        """Updates the miner's credibility based on the most recent set of validation_results.

        Requires: self.lock is held.
        """
        assert len(validation_results) > 0, "Must be provided at least 1 validation result."

        # Weight the current set of validation_results by the total content size validaed
        total_bytes_validated = sum(result.content_size_bytes_validated for result in validation_results)

        credibility = 0

        if total_bytes_validated > 0:
            credibility = sum(
                result.is_valid * result.content_size_bytes_validated for result in validation_results
            ) / float(total_bytes_validated)

        previous_credibility = self.miner_credibility[uid].clone().item()

        # Use EMA to update the miner's credibility.
        self.miner_credibility[uid] = (
            self.cred_alpha * credibility + (1 - self.cred_alpha) * self.miner_credibility[uid]
        )

        bt.logging.trace(
            f"""Evaluated Miner {uid}. Percent of bytes validated successfully this attempt={credibility * 100}.
                Previous Credibility={previous_credibility}. New Credibility={self.miner_credibility[uid].item()}."""
        )
