import threading
from typing import List, Optional
import torch
import bittensor as bt
import datetime as dt
from common.data import TimeBucket
from common.data_v2 import ScorableMinerIndex
from rewards.data_value_calculator import DataValueCalculator
from scraping.scraper import ValidationResult, S3ValidationResult


class MinerScorer:
    """Tracks the score of each miner and handles updates to the scores.

    Thread safe.
    """

    # Start new miner's at a credibility of 0.
    STARTING_CREDIBILITY = 0

    # Start new miners' S3 credibility at 0.375
    STARTING_S3_CREDIBILITY = 0.375

    # The exponent used to scale the miner's score by its credibility.
    _CREDIBILITY_EXP = 2.5

    ONDEMAND_MAX_CRED_PENALTY = 0.0075   # 0.75%
    ONDEMAND_BASE_REWARD = 200_000_000  # 200M


    def __init__(
        self,
        num_neurons: int,
        value_calculator: DataValueCalculator,
        cred_alpha: float = 0.15,
        s3_cred_alpha: float = 0.30
    ):
        # Tracks the raw scores of each miner. i.e. not the weights that are set on the blockchain.
        self.scores = torch.zeros(num_neurons, dtype=torch.float32)
        self.miner_credibility = torch.full(
            (num_neurons, 1), MinerScorer.STARTING_CREDIBILITY, dtype=torch.float32
        )
        # Keeps track of the amount of scorable bytes the miner had last time it was evaluated.
        self.scorable_bytes = torch.zeros(num_neurons, dtype=torch.float32)
        self.value_calculator = value_calculator
        self.cred_alpha = cred_alpha

        # Keeps track of the miner's current S3 boost based on the last S3 evaluation.
        self.s3_boosts = torch.zeros(num_neurons, dtype=torch.float32)
        self.s3_credibility = torch.full(
            (num_neurons, 1), MinerScorer.STARTING_S3_CREDIBILITY, dtype=torch.float32
        )
        self.s3_cred_alpha = s3_cred_alpha

        # Keeps track of the miner's OnDemand boost (EMA of recent OnDemand rewards)
        self.ondemand_boosts = torch.zeros(num_neurons, dtype=torch.float32)
        self.ondemand_alpha = 0.3

        # Make this class thread safe because it'll eventually be accessed by multiple threads.
        # One from the main validator evaluation loop and another from a background thread performing validation on user requests.
        self.lock = threading.Lock()

    def save_state(self, filepath):
        """Save the current state to the provided filepath."""
        with self.lock:
            torch.save(
                {
                    "scores": self.scores,
                    "credibility": self.miner_credibility,
                    "s3_boosts": self.s3_boosts,
                    "s3_credibility": self.s3_credibility,
                    "scorable_bytes": self.scorable_bytes,
                    "ondemand_boosts": self.ondemand_boosts,
                },
                filepath,
            )

    def load_state(self, filepath):
        """Load the state from the provided filepath."""
        state = torch.load(filepath, weights_only=True)
        with self.lock:
            self.scores = state["scores"]
            self.miner_credibility = state["credibility"]
            self.s3_boosts = state.get("s3_boosts", torch.zeros_like(self.scores))
            self.s3_credibility = state.get("s3_credibility", torch.full(
                (self.scores.size(0), 1), MinerScorer.STARTING_S3_CREDIBILITY, dtype=torch.float32
            ))
            self.ondemand_boosts = state.get("ondemand_boosts", torch.zeros_like(self.scores))

    def get_scores(self) -> torch.Tensor:
        """Returns the raw scores of all miners."""
        # Return a copy to ensure outside code can't modify the scores.
        with self.lock:
            return self.scores.clone()

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

    def zero_score_for_empty_file(self, uid: int, hotkey: str, empty_file_reason: str) -> None:
        """Zero all scores for a hotkey that uploaded empty files."""
        with self.lock:
            self.scores[uid] = 0.0
            self.miner_credibility[uid] = 0.0
            self.s3_boosts[uid] = 0.0
            self.s3_credibility[uid] = 0.0
            self.ondemand_boosts[uid] = 0.0

            bt.logging.error(
                f"EMPTY FILE EXPLOIT - UID {uid} ({hotkey}): All scores zeroed. "
                f"Reason: {empty_file_reason}"
            )

    def get_miner_credibility(self, uid: int) -> float:
        """Returns the credibility of miner 'uid'."""
        with self.lock:
            return self.miner_credibility[uid].item()

    def resize(self, num_neurons: int) -> None:
        """Resizes the score tensor to the new number of neurons.

        The new size must be greater than or equal to the current size.
        """
        with self.lock:
            assert num_neurons >= self.scores.size(
                0
            ), f"Tried to downsize the number of neurons from {self.scores.size(0)} to {num_neurons}"

            bt.logging.trace(
                f"Resizing MinerScorer from {self.scores.size(0)} to {num_neurons}"
            )

            to_add = num_neurons - self.scores.size(0)
            self.scores = torch.cat(
                [self.scores, torch.zeros(to_add, dtype=torch.float32)]
            )
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
            self.scorable_bytes = torch.cat(
                [self.scorable_bytes, torch.zeros(to_add, dtype=torch.float32)]
            )

            self.s3_boosts = torch.cat(
                [self.s3_boosts, torch.zeros(to_add, dtype=torch.float32)]
            )
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
            self.ondemand_boosts = torch.cat(
                [self.ondemand_boosts, torch.zeros(to_add, dtype=torch.float32)]
            )

    def update_s3_boost_and_cred(self, uid: int, s3_vali_percentage: float, job_match_failure = False) -> None:
        """Applies a fixed boost to the scaled score if the miner has passed S3 validation."""
        max_boost = 200 * 10**6  # Increased to 200M from 120M
        self.s3_boosts[uid] = s3_vali_percentage/100 * max_boost

        # If miners fail job match validation, immediately reset credibility to 0 (no EMA)
        # Otherwise, use EMA to update credibility
        if job_match_failure:
            bt.logging.info(f"S3 Job Match Failure for miner {uid}: Setting credibility to 0. S3 uploads must match their respective job request params.")
            self.s3_credibility[uid] = 0.0
        else:
            self.s3_credibility[uid] = min(1, s3_vali_percentage/100 * self.s3_cred_alpha + (1-self.s3_cred_alpha) * self.s3_credibility[uid])

        bt.logging.info(
            f"After S3 evaluation for miner {uid}: Raw S3 Boost = {float(self.s3_boosts[uid])}. S3 Credibility = {float(self.s3_credibility[uid])}."
        )

    def apply_ondemand_penalty(self, uid: int, mult_factor: float):
        """Applies a credibility penalty to a given miner based on their ondemand result"""
        with self.lock:
            cred_penalty = MinerScorer.ONDEMAND_MAX_CRED_PENALTY * mult_factor
            old_cred = float(self.miner_credibility[uid])

            # Apply credibility penalty
            self.miner_credibility[uid] = max(self.miner_credibility[uid] - cred_penalty, 0)
            new_cred = float(self.miner_credibility[uid])

            # EMA the ondemand boost with 0 reward for failures
            old_boost = float(self.ondemand_boosts[uid])
            failure_reward = 0
            self.ondemand_boosts[uid] = (
                self.ondemand_alpha * failure_reward +
                (1 - self.ondemand_alpha) * self.ondemand_boosts[uid]
            )
            new_boost = float(self.ondemand_boosts[uid])

            # Adjust score based on the credibility ratio change
            if old_cred > 0:
                cred_ratio = (new_cred / old_cred) ** MinerScorer._CREDIBILITY_EXP
                old_score = float(self.scores[uid])
                self.scores[uid] *= cred_ratio

                bt.logging.info(
                    f"OnDemand penalty for Miner {uid}: "
                    f"Credibility {old_cred:.4f} -> {new_cred:.4f}, "
                    f"Boost {old_boost:.2f} -> {new_boost:.2f}, "
                    f"Score {old_score:.2f} -> {float(self.scores[uid]):.2f}"
                )
            else:
                bt.logging.info(
                    f"OnDemand penalty for Miner {uid}: Credibility already at 0, "
                    f"Boost {old_boost:.2f} -> {new_boost:.2f}"
                )

    def apply_ondemand_reward(
        self,
        uid: int,
        speed_multiplier: float,
        volume_multiplier: float
    ):
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
                self.ondemand_alpha * raw_reward +
                (1 - self.ondemand_alpha) * self.ondemand_boosts[uid]
            )
            new_boost = float(self.ondemand_boosts[uid])

            bt.logging.info(
                f"OnDemand reward for Miner {uid}: "
                f"Speed={speed_multiplier:.3f}, Volume={volume_multiplier:.3f}, "
                f"Raw reward={raw_reward:.0f}, "
                f"OnDemand boost (EMA'd): {old_boost:.0f} -> {new_boost:.0f}"
            )

    def on_miner_evaluated(
        self,
        uid: int,
        index: Optional[ScorableMinerIndex],
        validation_results: List[ValidationResult]
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
                current_time_bucket = TimeBucket.from_datetime(
                    dt.datetime.now(tz=dt.timezone.utc)
                )
                for bucket in index.scorable_data_entity_buckets:
                    score += self.value_calculator.get_score_for_data_entity_bucket(
                        bucket, current_time_bucket
                    )

                # If the score has increased since the last eval, decrease credibility so that the
                # new score remains unchanged. i.e. "you've told us you now have more valuable data, prove it".
                # Note: After this step we then update the miner's credibility again, so if they passed
                # validation this time then their score will increase.
                previous_raw_score = self.scorable_bytes[uid].item()
                if previous_raw_score > 0 and score > previous_raw_score:
                    previous_cred = self.miner_credibility[uid].item()
                    cred_scalar = (previous_raw_score / score) ** (
                        1 / MinerScorer._CREDIBILITY_EXP
                    )
                    self.miner_credibility[uid] *= cred_scalar
                    bt.logging.debug(
                        f"Miner {uid}'s scorable bytes changed from {previous_raw_score} to {score}. Credibility changed from {previous_cred} to {self.miner_credibility[uid].item()}."
                    )

                # Record raw score for next time.
                self.scorable_bytes[uid] = score

                # Awarding the miner their S3 boost based on their last S3 evaluation.
                s3_boost = self.s3_boosts[uid] * self.s3_credibility[uid]
                score += s3_boost
                bt.logging.info(f"Awarded Miner {uid} a S3 boost of {float(s3_boost)} based off of the last performed S3 evaluation, adjusting the score to {float(score)}.")

                # Awarding the miner their OnDemand boost based on recent on-demand performance.
                ondemand_boost = float(self.ondemand_boosts[uid])
                score += ondemand_boost
                bt.logging.info(f"Awarded Miner {uid} an OnDemand boost of {ondemand_boost:.0f} based on recent on-demand performance, adjusting the score to {float(score)}.")

                # Now update the credibility again based on the current validation results.
                self._update_credibility(uid, validation_results)

                # Finally, scale the miner's score by its credibility to the power of 2.5.
                score *= self.miner_credibility[uid] ** MinerScorer._CREDIBILITY_EXP

            self.scores[uid] = score

            bt.logging.success(
                f"Evaluated Miner {uid}. Score={self.scores[uid].item()}. Credibility={self.miner_credibility[uid].item()}."
            )

    def _update_credibility(self, uid: int, validation_results: List[ValidationResult]):
        """Updates the miner's credibility based on the most recent set of validation_results.

        Requires: self.lock is held.
        """
        assert (
            len(validation_results) > 0
        ), "Must be provided at least 1 validation result."

        # Weight the current set of validation_results by the total content size validaed
        total_bytes_validated = sum(
            result.content_size_bytes_validated for result in validation_results
        )

        credibility = 0

        if total_bytes_validated > 0:
            credibility = sum(
                result.is_valid * result.content_size_bytes_validated
                for result in validation_results
            ) / float(total_bytes_validated)

        previous_credibility = self.miner_credibility[uid].clone().item()

        # Use EMA to update the miner's credibility.
        self.miner_credibility[uid] = (
            self.cred_alpha * credibility
            + (1 - self.cred_alpha) * self.miner_credibility[uid]
        )

        bt.logging.trace(
            f"""Evaluated Miner {uid}. Percent of bytes validated succesfully this attempt={credibility * 100}. 
                Previous Credibility={previous_credibility}. New Credibility={self.miner_credibility[uid].item()}."""
        )
