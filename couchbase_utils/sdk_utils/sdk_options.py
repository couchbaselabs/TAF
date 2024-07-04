from datetime import timedelta

from couchbase import options
from couchbase.durability import (ServerDurability, DurabilityLevel,
                                  ClientDurability, ReplicateTo, PersistTo)
from couchbase.subdocument import StoreSemantics

from constants.sdk_constants.java_client import SDKConstants


class SDKOptions(object):
    @staticmethod
    def get_duration(time, time_unit):
        time_unit = time_unit.lower()
        if time_unit == SDKConstants.TimeUnit.MILLISECONDS:
            return timedelta(milliseconds=time)
        if time_unit == SDKConstants.TimeUnit.MINUTES:
            return timedelta(minutes=time)
        if time_unit == SDKConstants.TimeUnit.HOURS:
            return timedelta(hours=time)
        if time_unit == SDKConstants.TimeUnit.DAYS:
            return timedelta(days=time)
        return timedelta(seconds=time)

    @staticmethod
    def get_persist_to(persist_to):
        try:
            persist_list = [PersistTo.NONE, PersistTo.ONE, PersistTo.TWO,
                            PersistTo.THREE]
            return persist_list[persist_to]
        except IndexError:
            return PersistTo.NONE

    @staticmethod
    def get_replicate_to(replicate_to):
        try:
            replicate_list = [ReplicateTo.NONE, ReplicateTo.ONE,
                              ReplicateTo.TWO, ReplicateTo.THREE]
            return replicate_list[replicate_to]
        except IndexError:
            return ReplicateTo.NONE

    @staticmethod
    def get_durability_level(durability_level):
        durability_level = durability_level.upper()
        if durability_level == SDKConstants.DurabilityLevel.MAJORITY:
            return ServerDurability(level=DurabilityLevel.MAJORITY)

        if durability_level == \
                SDKConstants.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE:
            return ServerDurability(
                level=DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE)

        if (durability_level ==
                SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY):
            return ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        return ServerDurability(level=DurabilityLevel.NONE)

    @staticmethod
    def get_scan_options(timeout=5,
                         time_unit=SDKConstants.TimeUnit.SECONDS):
        raise NotImplementedError()

    # Document operations' getOptions APIs
    @staticmethod
    def get_insert_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                           persist_to=0, replicate_to=0,
                           timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                           durability=""):
        d_options = SDKOptions.get_durability_level(durability)
        if persist_to or replicate_to:
            d_options = ClientDurability(
                replicate_to=ReplicateTo(replicate_to),
                persist_to=PersistTo(persist_to))
        return options.InsertOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit),
            expiry=SDKOptions.get_duration(exp, exp_unit),
            durability=d_options)

    @staticmethod
    def get_read_options(timeout, time_unit=SDKConstants.TimeUnit.SECONDS,
                         with_expiry=False):
        return options.GetOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit),
            with_expiry=with_expiry)

    @staticmethod
    def get_upsert_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                           persist_to=0, replicate_to=0,
                           timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                           durability="", preserve_expiry=False):
        d_options = SDKOptions.get_durability_level(durability)
        if persist_to or replicate_to:
            d_options = ClientDurability(
                replicate_to=ReplicateTo(replicate_to),
                persist_to=PersistTo(persist_to))
        return options.UpsertOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit),
            expiry=SDKOptions.get_duration(exp, exp_unit),
            durability=d_options, preserve_expiry=preserve_expiry)

    @staticmethod
    def get_remove_options(persist_to=0, replicate_to=0,
                           timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                           durability="", cas=0):
        d_options = SDKOptions.get_durability_level(durability)
        if persist_to or replicate_to:
            d_options = ClientDurability(
                replicate_to=ReplicateTo(replicate_to),
                persist_to=PersistTo(persist_to))
        params = {"timeout": SDKOptions.get_duration(timeout, time_unit),
                  "durability": d_options}
        if cas:
            params["cas"] = cas
        return options.RemoveMultiOptions(**params)

    @staticmethod
    def get_replace_options(persist_to=0, replicate_to=0,
                            timeout=5,
                            time_unit=SDKConstants.TimeUnit.SECONDS,
                            durability="", cas=0, preserve_expiry=None):
        d_options = SDKOptions.get_durability_level(durability)
        if persist_to or replicate_to:
            d_options = ClientDurability(
                replicate_to=ReplicateTo(replicate_to),
                persist_to=PersistTo(persist_to))
        params = {"timeout": SDKOptions.get_duration(timeout, time_unit),
                  "durability": d_options}
        if cas:
            params["cas"] = cas
        if preserve_expiry is not None:
            params["preserve_expiry"] = preserve_expiry
        return options.ReplaceOptions(**params)

    @staticmethod
    def get_touch_options(timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS):
        return options.UpsertOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit))

    @staticmethod
    def get_mutate_in_options(cas=0,
                              timeout=5,
                              time_unit=SDKConstants.TimeUnit.SECONDS,
                              persist_to=0, replicate_to=0, durability="",
                              store_semantics=None,
                              preserve_expiry=None,
                              access_deleted=None,
                              create_as_deleted=False):
        d_options = SDKOptions.get_durability_level(durability)
        if persist_to or replicate_to:
            d_options = ClientDurability(
                replicate_to=ReplicateTo(replicate_to),
                persist_to=PersistTo(persist_to))
        params = {"timeout": SDKOptions.get_duration(timeout, time_unit),
                  "durability": d_options}
        if cas:
            params["cas"] = cas
        if preserve_expiry is not None:
            params["preserve_expiry"] = preserve_expiry
        if store_semantics:
            params["store_semantics"] = \
                StoreSemantics.__getitem__(store_semantics)
        if access_deleted is not None:
            raise NotImplementedError()
        return options.MutateInOptions(**params)

    @staticmethod
    def get_insert_multi_options(
            exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
            persist_to=0, replicate_to=0,
            timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
            durability=""):
        d_options = SDKOptions.get_durability_level(durability)
        if persist_to or replicate_to:
            d_options = ClientDurability(
                replicate_to=ReplicateTo(replicate_to),
                persist_to=PersistTo(persist_to))
        return options.InsertMultiOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit),
            expiry=SDKOptions.get_duration(exp, exp_unit),
            durability=d_options)

    @staticmethod
    def get_upsert_multi_options(
            exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
            persist_to=0, replicate_to=0,
            timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
            durability="", preserve_expiry=False):
        d_options = SDKOptions.get_durability_level(durability)
        if persist_to or replicate_to:
            d_options = ClientDurability(
                replicate_to=ReplicateTo(replicate_to),
                persist_to=PersistTo(persist_to))
        return options.UpsertMultiOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit),
            expiry=SDKOptions.get_duration(exp, exp_unit),
            durability=d_options, preserve_expiry=preserve_expiry)

    @staticmethod
    def get_replace_multi_options(
            exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
            persist_to=0, replicate_to=0,
            timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
            durability="", preserve_expiry=False):
        d_options = SDKOptions.get_durability_level(durability)
        if persist_to or replicate_to:
            d_options = ClientDurability(
                replicate_to=ReplicateTo(replicate_to),
                persist_to=PersistTo(persist_to))
        o = options.ReplaceMultiOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit),
            expiry=SDKOptions.get_duration(exp, exp_unit),
            durability=d_options, preserve_expiry=preserve_expiry)
        return o

    @staticmethod
    def get_remove_multi_options(
            timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
            persist_to=0, replicate_to=0, durability=""):
        d_options = SDKOptions.get_durability_level(durability)
        if persist_to or replicate_to:
            d_options = ClientDurability(
                replicate_to=ReplicateTo(replicate_to),
                persist_to=PersistTo(persist_to))
        return options.RemoveMultiOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit),
            durability=d_options)

    @staticmethod
    def get_read_multi_options(
            timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
            with_expiry=False):
        return options.GetMultiOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit),
            with_expiry=with_expiry)

    @staticmethod
    def get_touch_multi_options(
            timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS):
        return options.TouchMultiOptions(
            timeout=SDKOptions.get_duration(timeout, time_unit))
