from math import isclose
from random import random
from typing import Tuple, List, Dict, Union, Optional

from metaflow import FlowSpec, step


POTENTIAL_TAGS: Tuple[str, ...] = ('TATA', 'TOTO', 'TUTU', 'TAXI')


class _(FlowSpec):
    @step
    def tag_transactions(self):
        self.tags: List[Dict[str, Union[int, Optional[str]]]] = [
            {
                'transaction_id': transaction['id'],
                'tag': tag,
            }
            for transaction in self.transactions
            for tags in (tuple(tag for tag in POTENTIAL_TAGS if tag in transaction['wording']),)
            for tag in (tags if len(tags) == 1 else (None,))
        ]
        self.next(self.publish_tags)

    @step
    def publish_tags(self):
        pass  # self.tags

    @step
    def annotate_transactions(self):
        self.annotations: List[Dict[str, Union[int, str]]] = [
            {
                'transaction_id': transaction['id'],
                'annotation': (
                    'LARGE SALE' if amount > 300. else
                    'SALE' if amount > 0. else
                    'EXPENSE' if amount < -200. else
                    'SMALL EXPENSE'
                ),
            }
            for transaction in self.transactions
            for amount in (transaction['amount'],)
            if not isclose(amount, 0.) and random() > 1e-4
        ]
        self.next(self.publish_annotations)

    @step
    def publish_annotations(self):
        pass  # self.annotations
