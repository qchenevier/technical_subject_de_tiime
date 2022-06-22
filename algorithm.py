from math import isclose
from random import random
from typing import Tuple, List, Dict, Union, Optional
from io import StringIO

from metaflow import FlowSpec, step, IncludeFile, Parameter, catch
import pandas as pd
from pathlib import Path
import sqlite_utils

POTENTIAL_TAGS: Tuple[str, ...] = ("TATA", "TOTO", "TUTU", "TAXI")
CURRENT_DIRECTORY = Path(__file__).parent


class TagAndAnnotateTransactionsFlow(FlowSpec):
    """
    A flow to annotate and tag transations.

    The flow performs the following steps in 2 parallel streams:

    1) Reads the CSV to get the transactions

    Stream A:
    1) Tags transactions
    2) Publish tags to a SQL database

    Stream B:
    1) Annotates transactions
    2) Publish annotations to a SQL database
    """

    transaction_file = IncludeFile(
        "transaction_file",
        help="The path to a transaction file.",
        default=str(CURRENT_DIRECTORY / "transactions.csv"),
    )
    database_filepath = Parameter(
        "database_filepath",
        help="The path to a database file.",
        default=str(CURRENT_DIRECTORY / "database.db"),
    )

    @step
    def start(self):
        """Start"""
        self.next(self.read_csv)

    @catch(var="read_csv_failed")
    @step
    def read_csv(self):
        """
        Parse the CSV file and load the values into a list of dictionaries.
        """
        raise Exception("Failure simulation")

        df_transactions = pd.read_csv(StringIO(self.transaction_file))
        self.transactions = df_transactions.to_dict(orient="records")
        self.next(self.tag_transactions, self.annotate_transactions)

    @catch(var="tag_transactions_failed")
    @step
    def tag_transactions(self):
        """Tag transations
        search for the tags of POTENTIAL_TAGS (global parameter) in the transaction
        returns a list of {transaction_id: tag} pairs
        """
        if not self.read_csv_failed:
            self.tags: List[Dict[str, Union[int, Optional[str]]]] = [
                {
                    "transaction_id": transaction["id"],
                    "tag": tag,
                }
                for transaction in self.transactions
                for tags in (
                    tuple(
                        tag
                        for tag in POTENTIAL_TAGS
                        if tag in transaction["wording"]
                    ),
                )
                for tag in (tags if len(tags) == 1 else (None,))
            ]
        self.next(self.publish_tags)

    @catch(var="publish_tags_failed")
    @step
    def publish_tags(self):
        """Publish tags in SQL database"""
        if not self.tag_transactions_failed and not self.read_csv_failed:
            db = sqlite_utils.Database(self.database_filepath)
            db["tags"].insert_all(self.tags, pk="transaction_id", replace=True)
        self.next(self.join)

    @catch(var="annotate_transactions_failed")
    @step
    def annotate_transactions(self):
        """Annotates the transactions
        Annotates with 4 classes based on the amount of the transaction
        returns a list of {transaction_id: annotation} pairs"""
        if not self.read_csv_failed:
            self.annotations: List[Dict[str, Union[int, str]]] = [
                {
                    "transaction_id": transaction["id"],
                    "annotation": (
                        "LARGE SALE"
                        if amount > 300.0
                        else "SALE"
                        if amount > 0.0
                        else "EXPENSE"
                        if amount < -200.0
                        else "SMALL EXPENSE"
                    ),
                }
                for transaction in self.transactions
                for amount in (transaction["amount"],)
                if not isclose(amount, 0.0) and random() > 1e-4
            ]
        self.next(self.publish_annotations)

    @catch(var="publish_annotations_failed")
    @step
    def publish_annotations(self):
        """Publish annotations in SQL database"""
        if not self.annotate_transactions_failed and not self.read_csv_failed:
            db = sqlite_utils.Database(self.database_filepath)
            db["annotations"].insert_all(
                self.annotations, pk="transaction_id", replace=True
            )
        self.next(self.join)

    @step
    def join(self, inputs):
        """Joins the different streams"""
        self.merge_artifacts(inputs, exclude=["_catch_exception"])
        self.next(self.end)

    @step
    def end(self):
        """End"""
        failures = [
            "read_csv_failed",
            "tag_transactions_failed",
            "publish_tags_failed",
            "annotate_transactions_failed",
            "publish_annotations_failed",
        ]
        flow_failed = False
        for failure in failures:
            if self.__getattr__(failure):
                print(f"A task failed: {failure}")
                flow_failed = True
        if flow_failed:
            print("Flow failed")
        print("Finished flow")


if __name__ == "__main__":
    TagAndAnnotateTransactionsFlow()

# TODO: send emails using twilio/sendgrid to redirect to the logs of the failures
