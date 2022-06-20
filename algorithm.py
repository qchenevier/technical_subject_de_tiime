from math import isclose
from random import random
from typing import Tuple, List, Dict, Union, Optional
from io import StringIO

from metaflow import FlowSpec, step, IncludeFile
import pandas as pd
from pathlib import Path


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

    @step
    def start(self):
        """Start"""
        self.next(self.read_csv)

    @step
    def read_csv(self):
        """
        Parse the CSV file and load the values into a list of dictionaries.
        """
        df_transactions = pd.read_csv(StringIO(self.transaction_file))
        print(df_transactions.head())
        self.transactions = df_transactions.to_dict(orient="records")

        self.next(self.tag_transactions, self.annotate_transactions)

    @step
    def tag_transactions(self):
        """Tag transations
        search for the tags of POTENTIAL_TAGS (global parameter) in the transaction
        returns a list of {transaction_id: tag} pairs
        """
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

    @step
    def publish_tags(self):
        """Publish tags in SQL database
        NOT IMPLEMENTED YET (only prints)
        """
        print(self.tags)
        self.next(self.join)

    @step
    def annotate_transactions(self):
        """Annotates the transactions
        Annotates with 4 classes based on the amount of the transaction
        returns a list of {transaction_id: annotation} pairs"""
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

    @step
    def publish_annotations(self):
        """Publish annotations in SQL database
        NOT IMPLEMENTED YET (only prints)
        """
        print(self.annotations)
        self.next(self.join)

    @step
    def join(self, inputs):
        """Joins the different streams"""
        self.next(self.end)

    @step
    def end(self):
        """Start"""
        print("Finished flow")


if __name__ == "__main__":
    TagAndAnnotateTransactionsFlow()

# TODO: put data in database (not SQlite but tinydb)
# TODO: monitor failures
