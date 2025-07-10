import threading
from typing import List, Optional, Dict
import torch
import bittensor as bt
import datetime as dt
from common.data import TimeBucket
from common.data_v2 import ScorableMinerIndex
from rewards.data_value_calculator import DataValueCalculator
from scraping.scraper import ValidationResult, HFValidationResult, S3ValidationResult


class MinerScorer:
    """Tracks the score of each miner and handles updates to the scores.

    Thread safe.
    """

    # Start new miner's at a credibility of 0.
    STARTING_CREDIBILITY = 0

    # Start new miners' HF credibility at 0.375
    STARTING_HF_CREDIBILITY = 0.375

    # Start new miners' S3 credibility at 0.375
    STARTING_S3_CREDIBILITY = 0.375

    # The exponent used to scale the miner's score by its credibility.
    _CREDIBILITY_EXP = 2.5

    def __init__(
        self,
        num_neurons: int,
        value_calculator: DataValueCalculator,
        cred_alpha: float = 0.15,
        hf_cred_alpha: float = 0.20,
        s3_cred_alpha: float = 0.20
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

        # Keeps track of the miner's current HF boost based on the last HF evaluation.
        self.hf_boosts = torch.zeros(num_neurons, dtype=torch.float32)
        self.hf_credibility = torch.full(
            (num_neurons, 1), MinerScorer.STARTING_HF_CREDIBILITY, dtype=torch.float32
        )
        self.hf_cred_alpha = hf_cred_alpha

        # Keeps track of the miner's current S3 boost based on the last S3 evaluation.
        self.s3_boosts = torch.zeros(num_neurons, dtype=torch.float32)
        self.s3_credibility = torch.full(
            (num_neurons, 1), MinerScorer.STARTING_S3_CREDIBILITY, dtype=torch.float32
        )
        self.s3_cred_alpha = s3_cred_alpha

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
                    "hf_boosts": self.hf_boosts,
                    "hf_credibility": self.hf_credibility,
                    "s3_boosts": self.s3_boosts,
                    "s3_credibility": self.s3_credibility,
                    "scorable_bytes": self.scorable_bytes,
                },
                filepath,
            )

    def load_state(self, filepath):
        """Load the state from the provided filepath."""
        state = torch.load(filepath, weights_only=True)
        with self.lock:
            self.scores = state["scores"]
            self.miner_credibility = state["credibility"]
            self.hf_boosts = state["hf_boosts"]
            self.hf_credibility = state["hf_credibility"]
            # Handle backward compatibility for S3 fields
            self.s3_boosts = state.get("s3_boosts", torch.zeros_like(self.scores))
            self.s3_credibility = state.get("s3_credibility", torch.full(
                (self.scores.size(0), 1), MinerScorer.STARTING_S3_CREDIBILITY, dtype=torch.float32
            ))

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
            self.hf_boosts[uid] = 0.0
            self.hf_credibility[uid] = MinerScorer.STARTING_HF_CREDIBILITY
            self.s3_boosts[uid] = 0.0
            self.s3_credibility[uid] = MinerScorer.STARTING_S3_CREDIBILITY

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
            self.hf_boosts = torch.cat(
                [self.hf_boosts, torch.zeros(to_add, dtype=torch.float32)]
            )
            self.hf_credibility = torch.cat(
                [
                    self.hf_credibility,
                    torch.full(
                        (to_add, 1),
                        MinerScorer.STARTING_HF_CREDIBILITY,
                        dtype=torch.float32,
                    ),
                ]
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

    def update_hf_boost_and_cred(self, uid: int, hf_vali_percentage: float) -> None:
        """Applies a fixed boost to the scaled score if the miner has passed HF validation."""
        max_boost = 10 * 10**6
        self.hf_boosts[uid] = hf_vali_percentage/100 * max_boost
        self.hf_credibility[uid] = min(1, hf_vali_percentage/100 * self.hf_cred_alpha + (1-self.hf_cred_alpha) * self.hf_credibility[uid])
        bt.logging.info(
            f"After HF evaluation for miner {uid}: Raw HF Boost = {float(self.hf_boosts[uid])}. HF Credibility = {float(self.hf_credibility[uid])}."
        )


    def apply_ondemand_penalty(self, uid: int, mult_factor: float):
        """Applies a 5% credibility penalty to a given miner"""
        cred_penalty = 0.05 * mult_factor
        adj_cred = max(self.miner_credibility[uid] - cred_penalty, 0)
        bt.logging.info(f"After {100*cred_penalty:.2f}% OnDemand penalty, Miner {uid} credibility decreased from {float(self.miner_credibility[uid])} to {adj_cred}.")
        self.miner_credibility[uid] = adj_cred

    def on_s3_evaluated(
        self,
        uid: int,
        completed_jobs: List[Dict],
        validation_results: List
    ) -> None:
        """Notifies the scorer that a miner's S3 data has been evaluated and should have its score updated.
        Mirrors the on_miner_evaluated approach but for S3 validation.

        Args:
            uid (int): The miner's UID.
            completed_jobs (List[Dict]): List of completed job data with job_id, data_size, etc.
            validation_results (List[S3ValidationResult]): The results of content validation performed on sampled S3 data.
        """
        
        with self.lock:
            s3_score = 0.0

            # Calculate raw S3 score based on job completion (mimicking MinerIndex scoring)
            for job_data in completed_jobs:
                job_id = job_data['job_id']
                data_size_bytes = job_data['total_size_bytes']
                
                # Get job data from dynamic desirability model
                job_info = self.value_calculator.model.get_job_data_by_id(job_id)
                if job_info:
                    data_source = job_info.get('data_source')
                    job_weight = job_info.get('job_weight', 0.0)

                    data_source_weight = self.value_calculator.model.get_data_source_weight(data_source)

                    job_score = data_source_weight * job_weight * data_size_bytes
                    s3_score += job_score

            # Apply S3 boost calculation (mirrors on_miner_evaluated approach for weighted score)
            if validation_results:
                # Update S3 credibility based on validation results  
                self._update_s3_credibility(uid, validation_results)
                
                # Calculate S3 boost based on score and credibility
                s3_boost = s3_score * self.s3_credibility[uid]
                self.s3_boosts[uid] = s3_boost
                
                bt.logging.info(f"Miner {uid} S3 evaluation completed. S3 Score: {s3_score:.0f}, "
                              f"S3 Credibility: {float(self.s3_credibility[uid]):.3f}, "
                              f"S3 Boost: {float(s3_boost):.0f}")
            else:
                bt.logging.info(f"Miner {uid} S3 evaluation completed. S3 Score: {s3_score:.0f}, "
                              f"No validation results provided")

    def _update_s3_credibility(self, uid: int, validation_results: List):
        """Updates the miner's S3 credibility based on the most recent set of validation_results.

        Requires: self.lock is held.
        """
        assert (
            len(validation_results) > 0
        ), "Must be provided at least 1 S3 validation result."

        total_bytes_validated = sum(
            result.content_size_bytes_validated for result in validation_results
        )

        credibility = 0

        # only calculate credibility if we have > 0 validated bytes
        if total_bytes_validated > 0:
            valid_bytes = sum(
                result.is_valid * result.content_size_bytes_validated
                for result in validation_results
            )
            credibility = valid_bytes / float(total_bytes_validated)
        else:
            # If no bytes were validated, treat as failed validation
            credibility = 0
            bt.logging.warning(f"S3 validation for miner {uid} had zero bytes validated")

        previous_credibility = self.s3_credibility[uid].clone().item()

        # Use EMA to update the miner's S3 credibility.
        self.s3_credibility[uid] = (
            self.s3_cred_alpha * credibility
            + (1 - self.s3_cred_alpha) * self.s3_credibility[uid]
        )

        bt.logging.info(
            f"Miner {uid}'s S3 credibility updated from {previous_credibility} to {self.s3_credibility[uid].item()} "
            f"based on {len(validation_results)} validation results with {total_bytes_validated} bytes validated."
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
            hf_validation_result (Optional, HFValidationResult): The overall result from a validation process on a 10,000 row sample from a miner's HF dataset. 
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
                
                # Awarding the miner their HF boost based on their last HF evaluation. 
                hf_boost = self.hf_boosts[uid] * self.hf_credibility[uid]
                score += hf_boost
                bt.logging.info(f"Awarded Miner {uid} a HF boost of {float(hf_boost)} based off of the last performed HF evaluation.")
                
                # Awarding the miner their S3 boost based on their last S3 evaluation.
                s3_boost = self.s3_boosts[uid] * self.s3_credibility[uid]
                score += s3_boost
                bt.logging.info(f"Awarded Miner {uid} a S3 boost of {float(s3_boost)} based off of the last performed S3 evaluation, adjusting the score to {float(score)}.")

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
