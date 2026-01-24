import math
from typing import Tuple
from typing import Any, Dict, Optional
from collections_helper.collections_spec_constants import MetaConstants


class CollectionScopeNumberManager:

    def __init__(self, spec: Any, update_object: Dict[str, Any]) -> None:
        self._spec = spec
        self._update_object = update_object

        self.collection_scale = None
        self.max_possible_collection = None
        self.collection_factor = None
        self.update_spec = None
        self.current_max_collections = None
        self.setup()

    def setup(self) -> None:
        update_param = self._extract_update_parameters()
        self.max_possible_collection = update_param.get("max_possible_collection", None)
        self.collection_factor = update_param["collection_factor"]
        self.collection_scale = update_param["collection_scale"]
        self.update_spec = self.update_spec_possible()
        if self.update_spec:
            self.current_max_collections = math.ceil(self.collection_factor * self.max_possible_collection)

    def _extract_update_parameters(self) -> Dict[str, Optional[Any]]:
        update = self._update_object or {}

        return {
            "max_possible_collection": update.get("max_possible_collection", None),
            "collection_factor": update.get("collection_factor", None),
            "collection_scale": update.get("collection_scale", None),
        }

    def update_spec_possible(self) -> bool:
        if (self.collection_factor is None or
                self.collection_scale is None or
                self.max_possible_collection is None):
            return False
        if self.collection_scale > 1 or self.collection_scale <= 0:
            return False
        return True

    def get_final_multipliers(self, n: int, scale: float) -> Tuple[int, int]:
        if not (0 < scale <= 1):
            raise ValueError("scale must be in (0, 1]")

        factors = self.prime_factors(n)

        if len(factors) <= 1:
            return (n, 1)

        k = math.ceil(len(factors) * scale)

        collection_multiplier = 1
        scope_multiplier = 1

        for f in factors[-k:]:
            collection_multiplier *= f

        for f in factors[:-k]:
            scope_multiplier *= f

        return collection_multiplier, scope_multiplier

    def update_bucket_spec(self) -> Any:

        if self.update_spec is False:
            return self._spec

        if (self._spec.get(MetaConstants.NUM_SCOPES_PER_BUCKET, None)
                and self._spec.get(MetaConstants.NUM_COLLECTIONS_PER_SCOPE, None)):
            multiplying_factor = self.calculate_multiplying_factor(
                self._spec[MetaConstants.NUM_SCOPES_PER_BUCKET],
                self._spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE],
            )
            if multiplying_factor is not None:
                final_factors = self.get_final_multipliers(multiplying_factor,
                                                           self.collection_scale)
                self._spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE] = (
                            self._spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE] *
                            final_factors[0])
                self._spec[MetaConstants.NUM_SCOPES_PER_BUCKET] = \
                    (self._spec[MetaConstants.NUM_SCOPES_PER_BUCKET] * final_factors[1])

        if "buckets" in self._spec:
            for bucket in self._spec["buckets"]:
                bucket_spec = self._spec["buckets"][bucket]
                if (bucket_spec.get(MetaConstants.NUM_SCOPES_PER_BUCKET, None) and
                        bucket_spec.get(MetaConstants.NUM_COLLECTIONS_PER_SCOPE, None)):
                    multiplying_factor = self.calculate_multiplying_factor(
                        bucket_spec[MetaConstants.NUM_SCOPES_PER_BUCKET],
                        bucket_spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE],
                    )
                    if multiplying_factor is None:
                        continue
                    final_factors = self.get_final_multipliers(multiplying_factor, self.collection_scale)
                    bucket_spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE] = \
                        (bucket_spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE] * final_factors[0])
                    bucket_spec[MetaConstants.NUM_SCOPES_PER_BUCKET] = \
                        (bucket_spec[MetaConstants.NUM_SCOPES_PER_BUCKET] * final_factors[1])
        return self._spec

    def calculate_multiplying_factor(self, num_scopes: int , num_collections: int):
        if num_scopes == 0 or num_collections == 0:
            return None
        if num_scopes * num_collections > self.current_max_collections:
            return 1
        return self.current_max_collections // (num_scopes * num_collections)

    def prime_factors(self, n: int) -> list[int]:
        factors = []

        while n % 2 == 0:
            factors.append(2)
            n //= 2

        i = 3
        while i * i <= n:
            while n % i == 0:
                factors.append(i)
                n //= i
            i += 2

        if n > 1:
            factors.append(n)

        return factors


    def teardown(self) -> None:
        self.collection_scale = None
        self.max_possible_collection = None
