class CapellaScaleSpec(object):
    """
    Builds the POST .../clusters/<id>/specs payload for a Capella scale,
    starting from the cluster's current specs (GET .../specs).

    Pure data mapping, no HTTP.
    GET returns services / compute as strings; POST needs objects
    ({"type": ...}), else the API rejects the request with HTTP 400.
    """
    DATA_SERVICE = "kv"
    MIN_DATA_NODES = 3

    def __init__(self, current_specs, provider):
        self.current_specs = current_specs
        self.provider = provider

    @property
    def node_count(self):
        return sum(spec["count"] for spec in self.current_specs)

    def resize(self, delta, services=None):
        """
        :param delta: nodes to add (+) / remove (-) in the target group
        :param services: services of the target group (eg: ["kv"]).
                         Default: the group running the data service
        :return: specs payload for CapellaUtils.scale()
        """
        specs = [self._to_request(spec) for spec in self.current_specs]
        group = self._target_group(specs, services or [self.DATA_SERVICE])
        group["count"] += delta
        self._check_min_data_nodes(group)
        return {"specs": specs}

    def _to_request(self, spec):
        disk = spec["disk"]
        return {"count": spec["count"],
                "services": [{"type": s} for s in spec["services"]],
                "compute": {"type": spec["compute"]},
                "disk": {"type": disk["type"],
                         "sizeInGb": disk["sizeInGb"],
                         "iops": disk.get("iops")},
                "diskAutoScaling": {
                    "enabled": spec["diskAutoScaling"]["enabled"]},
                "provider": self.provider}

    @staticmethod
    def _target_group(specs, services):
        wanted = sorted(services)
        for spec in specs:
            if sorted(s["type"] for s in spec["services"]) == wanted:
                return spec
        raise ValueError("No service group running exactly %s" % wanted)

    def _check_min_data_nodes(self, group):
        is_data = any(s["type"] == self.DATA_SERVICE
                      for s in group["services"])
        if is_data and group["count"] < self.MIN_DATA_NODES:
            raise ValueError("Capella needs at least %s data nodes, "
                             "scale would leave %s"
                             % (self.MIN_DATA_NODES, group["count"]))
