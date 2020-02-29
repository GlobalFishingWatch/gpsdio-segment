from __future__ import division, print_function, absolute_import
from collections import namedtuple
import datetime as DT
import logging
import math

from .discrepancy import DiscrepancyCalculator

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

Track = namedtuple('Track', ['id', 'prefix', 'segments', 'count', 'decayed_count', 'is_active'])


log = logger.debug

EPSILON = 1e-6

def clip(x, l, h):
    if x < l:
        return l
    if x > h:
        return h
    return x

S_PER_HR = 60 * 60
S_PER_DAY = S_PER_HR * 24


class Stitcher(DiscrepancyCalculator):
    """Stitch segments together into coherent tracks.
    """
    
    # General parameters
    condense_interval = 32
    max_hypotheses = 16
    min_seg_size = 3 # segments shorter than this are dropped
    max_active_tracks = 8

    # Parameters controlling how effective hours are computed
    penalty_hours = 4
    hours_exp = 0.5
    buffer_hours = 1.0

    # Parameters used in computing costs
    buffer_count = 10
    max_discrepancy = 3000
    time_metric_scale_hours = 7 * 24
    max_average_knots = 50 
    max_overlap_hours = 2.0
    max_overlap_fraction = max_overlap_hours / 24.0
    base_track_cost = 1.0

    # Weights of various cost components
    # TODO: weight should be incorporated into costs
    count_weight = 0.1
    signature_weight = 1.0
    discrepancy_weight = 1.0
    overlap_weight = 1.0
    speed_weight = 1.0
    time_metric_weight = 1.0

    track_count_decay_per_day = 0.9
    msg_count_decacy_per_day = 0.99
    
    def __init__(self, **kwargs):
        for k in kwargs:
            # TODO: when interface stable, fix keys as in core.py
            self._update(k, kwargs)
    
    @staticmethod
    def aug_seg_id(dict_obj):
        """Return the segment id augmented with the date of this subsegment"""
        if dict_obj['seg_id'] is None:
            return None
        return dict_obj['seg_id'] + '-' + dict_obj['timestamp'].date().isoformat()        

    @staticmethod
    def add_track_ids(msgs, tracks):
        """Add track ids to a messages.

        Parameters
        ----------
        msgs : list of messages
            List of messages encoded as dictionaries
        tracks : list segments 

        """
        # TODO: eventually use objects where appropriate
        id_map = {}
        for track in tracks:
            track_id = Stitcher.aug_seg_id(track[0])
            for seg in track:
                id_map[Stitcher.aug_seg_id(seg)] = track_id
        for msg in msgs:
            try:
                msg['track_id'] = id_map.get(Stitcher.aug_seg_id(msg), None)
            except:
                print(msg)
                raise

    def _compute_signatures(self, segs):
        def get_sig(seg):
            return [
                dict(seg.transponders),
                dict(seg.shipnames),
                dict(seg.callsigns),
                dict(seg.imos)
            ]
        return {s.aug_id : get_sig(s) for s in segs}
    
    def filter_and_sort(self, segs, min_seg_size=1):
        def is_null(x):
            return x is None or str(x) == 'NaT'
        def has_messages(s):
            return not(is_null(s.last_msg_of_day.timestamp) and
                       is_null(s.first_msg_of_day.timestamp))
        segs = [x for x in segs if has_messages(x)]
        segs.sort(key=lambda x: (x.timestamp, x.first_msg_of_day.timestamp))
        keys = ['shipnames', 'callsigns', 'imos', 'transponders']
        identities = {}
        sizes = {}
        days = {}
        for seg in segs:
            seg_id = seg.id
            sizes[seg_id] = max(seg.msg_count, sizes.get(seg_id, 0))
            if seg_id not in identities:
                identities[seg_id] = {k : {} for k in keys}
                days[seg_id] = 0
            days[seg_id] += 1
            for k in keys:
                for v, cnt in getattr(seg, k):
                    identities[seg_id][k][v] = identities[seg_id][k].get(v, 0) + cnt

        for seg in segs:
            seg_id = seg.id
            assert seg_id is not None
            for k, sig in identities[seg_id].items():
                seg_sig = []
                for value, count in sig.items():
                    seg_sig.append({'value' : value, 'count' : count / days[seg_id]})
                seg = seg._replace(**{k : seg_sig})


        return [seg for seg in segs if sizes[seg.id] > min_seg_size]


    @staticmethod
    def seg_time_delta(seg0, seg1):
        return seg1.first_msg_of_day.timestamp - seg0.last_msg_of_day.timestamp

    def _tolerance(self, signature_count, n_tracks):
        # Tolerance *for new tracks*, so when this is lower we should be less
        # likely to form new tracks
        alpha = n_tracks / max(signature_count, 1)
        return math.exp(1 - alpha)


    @staticmethod
    def seg_duration(seg):
        return (seg.last_msg_of_day.timestamp - 
                seg.first_msg_of_day.timestamp)


    def signature_cost(self, seg1, seg2):
        signatures = self.signatures
        if seg1.id == seg2.id:
            # These two chunks are from the same segment, so 
            # say they match
            return 0.0

        sig1 = signatures[seg1.aug_id][:2]
        sig2 = signatures[seg2.aug_id][:2]
        # A perfect match of transponder type is not very specific since there only two types,
        # So we cap the specificity if the match is positive
        max_pos_specificities = [0.5, 0.99]

        match = []
        for a, b, mps in zip(sig1, sig2, max_pos_specificities):
            if len(a) and len(b):
                na = math.sqrt(sum([v**2 for v in a.values()]))
                nb = math.sqrt(sum([v**2 for v in b.values()]))
                if na and nb:
                    maxa = max(a.values()) / na
                    maxb = max(b.values()) / nb
                    cos = 0
                    for k in set(a) & set(b):
                        va = a[k] / na
                        vb = b[k] / nb
                        cos += va * vb
                    cos2 = 2 * cos ** 2 - 1
                    num_dims = len(set(a) | set(b))
                    # Specificity is an ad hoc measure of how much information the
                    # vector provides. From 0, when all components (identities) are
                    # equal to 1, when the vector has one identity (along an axis)
                    if num_dims == 1:
                        specificity = 1.0
                    else:
                        alpha = num_dims ** 0.5 / (num_dims ** 0.5 - 1)
                        beta = 1 / (num_dims ** 0.5 - 1)
                        specificity_a = alpha * maxa - beta
                        specificity_b = alpha * maxb - beta
                        specificity = specificity_a * specificity_b
                    if cos2 >= 0:
                        specificity *= mps
                    match.append((cos2, specificity))
                    continue
            else:
                match.append((0, 0))
        log('signature 1: %s', sig1)
        log('signature 2: %s', sig2)
        log('match_vector: %s', match)

        denom = sum(x[1] for x in match) + EPSILON
        # Raw metric is a value between -1 and 1
        raw_metric = sum((x[0] * x[1]) for x in match) / denom
        # Return a value between 0 and 1
        return self.signature_weight * 0.5 * (1 - raw_metric)
      

    def overlap_cost(self, seg1, seg2):
        msg0 = seg1.last_msg_of_day
        msg1 = seg2.first_msg_of_day
        overlapped = msg0.timestamp > msg1.timestamp
        if overlapped:
            msg0, msg1 = msg1, msg0
            def dt(s):
                return (s.last_msg_of_day.timestamp - 
                        s.first_msg_of_day.timestamp).total_seconds() / S_PER_HR
            dt0 = self.seg_duration(seg1).total_seconds() / S_PER_HR
            dt1 = self.seg_duration(seg2).total_seconds() / S_PER_HR
            max_oh = min(self.max_overlap_hours, 
                         self.max_overlap_fraction * dt0,
                         self.max_overlap_fraction * dt1)
            oh = self.compute_msg_delta_hours(msg0._asdict(), msg1._asdict()) 
            return self.overlap_weight * oh / max_oh
        else:
            return 0



    def compute_cost(self, seg1, seg2):
        sig_cost = self.signature_cost(seg1, seg2)
        overlap_cost = self.overlap_cost(seg1, seg2)

        hours = self.seg_time_delta(seg1, seg2).total_seconds() / S_PER_HR
        msg1 = seg1.last_msg_of_day
        msg2 = seg2.first_msg_of_day
        if hours < 0:
            # Overlap is already penalized, so swap seg1 and seg2
            msg1, msg2 = msg2, msg1
            hours = -hours
        penalized_hours = hours / (1 + (hours / self.penalty_hours) ** (1 - self.hours_exp))
        discrepancy = self.compute_discrepancy(msg1._asdict(), msg2._asdict(), penalized_hours)
        padded_hours = math.hypot(hours, self.buffer_hours)

        disc_cost = self.discrepancy_weight * discrepancy / self.max_discrepancy

        speed = discrepancy / padded_hours
        speed_cost = self.speed_weight * speed  / self.max_average_knots

        time_cost = self.time_metric_weight * (1 - self.time_metric_scale_hours / 
                        (self.time_metric_scale_hours + hours))


        return ( 
                 sig_cost +
                 disc_cost +
                 speed_cost +
                 time_cost +
                 overlap_cost
                 )


    @staticmethod
    def signatures_count(segs, sig_abs=10, sig_frac=0.05):
        """Check if this vessel has multiple ids based on the signature
        """
        keys = ['shipnames', 'callsigns', 'imos']
        identities = {k : {} for k in keys}
        sizes = {}
        days = {}
        for seg in segs:
            for k in keys:
                for (v, cnt) in getattr(seg, k):
                    identities[k][v] = identities[k].get(v, 0) + cnt
        counts = []
        for k in keys:
            total = 0
            for lbl, cnt in identities[k].items():
                total += cnt
            idents = 0
            for lbl, cnt in identities[k].items():
                if cnt > sig_abs and cnt > sig_frac * total:
                    idents += 1
            counts.append(idents)
        return max(counts)


    # def find_track_signatures(self, start_date, tracks, segs, lookback=30):
    #     end_range = start_date - DT.timedelta(days=1)
    #     start_range = start_date - DT.timedelta(days=lookback)
    #     segs = [x for x in segs if start_range <= x.timestamp <= end_range]
    #     seg_sigs = self._compute_signatures(segs)
    #     track_sigs = {}
    #     for track in tracks:
    #         track_id = track[0].aug_id
    #         sigs = [{}, {}, {}, {}]
    #         for seg in track:
    #             seg_id = seg.aug_id
    #             if seg_id in seg_sigs:
    #                 track_sigs
    #                 for i, (s1, s2) in enumerate(zip(seg_sigs[seg_id], sigs)):
    #                     for k in set(s1.keys()) | set(s2.keys()):
    #                         sigs[i][k] = s1.get(k, 0) + s2.get(k, 0) 
    #         track_sigs[track_id] = sigs

    #     return track_sigs


    def update_hypotheses(self, hypotheses, segment):
        updated = []
        for h in hypotheses:
            track_list = self.prune_tracks(h['tracks'])
            track_count = 0
            for i, track in enumerate(track_list):
                if not track.is_active:
                    continue
                new_list = list(track_list)
                last_seg = track.segments[-1] if track.segments else track.prefix[-1]
                days_since_track = (segment.first_msg_of_day.timestamp - 
                                    last_seg.last_msg_of_day.timestamp).total_seconds() / S_PER_DAY
                decayed_count = track.count * self.msg_count_decacy_per_day ** days_since_track
                new_list[i] = track._replace(segments=tuple(track.segments) + (segment,),
                                             count=track.count + segment.msg_count,
                                             decayed_count=decayed_count + segment.msg_count)
                if track.segments:
                    cost = h['cost'] + self.find_cost(track.segments[-1], segment)
                elif track.prefix:
                    cost = h['cost'] + self.find_cost(track.prefix[-1], segment)
                else:
                    # This occurs at startup so we give this the base track cost
                    cost = h['cost'] + self.base_track_cost
                updated.append({'cost' : cost, 'tracks' : new_list})
                if track.segments:
                    s_since_track = (segment.first_msg_of_day.timestamp - 
                                 track.segments[-1].last_msg_of_day.timestamp).total_seconds()
                else:
                    s_since_track = (segment.first_msg_of_day.timestamp - 
                                 track.prefix[-1].last_msg_of_day.timestamp).total_seconds()  
                days_since_track = s_since_track / S_PER_HR
                track_count += self.track_count_decay_per_day ** days_since_track
            new_list = list(track_list)
            new_list.append(Track(id=segment.aug_id, prefix=[], 
                                  segments=(segment,), count=segment.msg_count,
                                  decayed_count=segment.msg_count,
                                  is_active=True))
            tolerance = self._tolerance(self.signature_count, track_count)
            new_track_cost = self.base_track_cost #/ tolerance
            updated.append({'cost' : h['cost'] + new_track_cost, 'tracks' : new_list})
        return updated  

    _seg_joining_costs = {}

    def find_cost(self, seg0, seg1):
        key = (seg0.aug_id, seg1.aug_id)
        if key not in self._seg_joining_costs:
            self._seg_joining_costs[key] = self.compute_cost(seg0, seg1)
        return self._seg_joining_costs[key]

    def prune_hypotheses(self, hypotheses_list, n):
        def count_cost(h):
            return self.count_weight * sum(x.count ** 0.5 for x in h['tracks'])
        hypotheses_list.sort(key=lambda x: x['cost'] + count_cost(x))
        return hypotheses_list[:n]

    def condense_hypotheses(self, hypotheses_list):
        """
        If the same prefix occurs in all tracks it can be removed.
        """
        if len(hypotheses_list) == 0:
            return hypotheses_list

        # Check that prefixes are consistent and collect track_ids
        base_prefixes = {}
        track_ids = set()
        for hypothesis in hypotheses_list:
            for track in hypothesis['tracks']:
                track_ids.add(track.id)
                if track.id not in base_prefixes:
                    base_prefixes[track.id] = track.prefix
                assert track.prefix == base_prefixes[track.id], (
                        [x.aug_id for x in track.prefix], 
                        [x.aug_id for x in base_prefixes[track.id]])


        potential_prefixes = {}

        ids_seen = set()
        for track in hypotheses_list[0]['tracks']:
            ids_seen.add(track.id)
            potential_prefixes[track.id] = set(enumerate(track.segments))
        for track_id in (track_ids - ids_seen):
            potential_prefixes[track_id] = set()

        for hypothesis in hypotheses_list[1:]:
            ids_seen = set()
            for track in hypothesis['tracks']:
                ids_seen.add(track.id)
                potential_prefixes[track.id] = potential_prefixes[track.id] & set(enumerate(track.segments))
            for track_id in (track_ids - ids_seen):
                potential_prefixes[track_id] = set()

        prefix_indices = {}

        for id_, seg_set in potential_prefixes.items():
            segs_list = sorted(seg_set)
            if segs_list and segs_list[0][0] != 0:
                prefix_indices[id_] = 0
            else:
                for i, (ndx, seg) in enumerate(segs_list):
                    if i != ndx:
                        prefix_indices[id_] = i
                        break
                else:
                    prefix_indices[id_] = len(segs_list)
            assert [x[0] for x in segs_list[:prefix_indices[id_]]] == list(range(prefix_indices[id_]))

        for hypothesis in hypotheses_list:
            new_tracks = []
            for track in hypothesis['tracks']:
                ndx = prefix_indices[track.id]
                assert len(track.segments) >= ndx
                new_tracks.append(track._replace(
                    prefix = tuple(track.prefix) + tuple(track.segments[:ndx]),
                    segments = track.segments[ndx:]
                    ))
            hypothesis['tracks'] = new_tracks

        return hypotheses_list

    def prune_tracks(self, tracks):
        active_tracks = [x for x in tracks if x.is_active]
        if len(active_tracks) > self.max_active_tracks:
            active_tracks.sort(key = lambda x: x.decayed_count, reverse=True)
            inactive_tracks = [x for x in tracks if not x.is_active]
            pruned_tracks = inactive_tracks + active_tracks[:self.max_active_tracks]
            for track in active_tracks[self.max_active_tracks:]:
                pruned_tracks.append(track._replace(is_active=False))
            pruned_tracks = tuple(pruned_tracks)
        else:
            pruned_tracks = tracks
        return pruned_tracks


    # def cook_tracks(self, raw_tracks):
    #     tracks = []
    #     for raw in raw_tracks:
    #         count = 0
    #         decayed_count = 0
    #         last_dtime = None
    #         for seg in raw:
    #             # Another reason it would be better to pass around track objects TODO:
    #             if seg.timestamp is None:
    #                 continue # Placeholder
    #             count += seg.msg_count
    #             if last_dtime is not None:
    #                 s_since_track = (seg.first_msg_of_day.timestamp - 
    #                                  last_dtime).total_seconds() 
    #                 days_since_track = s_since_track / S_PER_HR
    #             else:
    #                 days_since_track = 0
    #             decayed_count = decayed_count * self.msg_count_decacy_per_day ** days_since_track
    #             count += seg.msg_count
    #             decayed_count += seg.msg_count
    #         tracks.append(Track(raw[0].aug_id, [], tuple(raw), count, decayed_count, 
    #                             last_msg_timestamp=
    #                             is_active=True))
    #     return tracks

    def create_tracks(self, start_date, tracks, track_sigs, segs):
        self.signature_count = max(self.signatures_count(segs), 1)

        segs = self.filter_and_sort(segs, self.min_seg_size)
        self.signatures  = self._compute_signatures(segs)
        # Now remove all segments that occur before today:
        segs = [x for x in segs if x.timestamp.date() >= start_date.date()]

        # New below here

        hypotheses = [{'cost' : 0, 'tracks' : tracks}]

        for i, seg in enumerate(segs):
            if not self.seg_duration(seg).total_seconds() > 0:
                continue
            hypotheses = self.update_hypotheses(hypotheses, seg)
            if i % self.condense_interval == 0:
                hypotheses = self.condense_hypotheses(hypotheses)
            hypotheses = self.prune_hypotheses(hypotheses, self.max_hypotheses)
        [final_hypothesis] = self.prune_hypotheses(hypotheses, 1)

        tracks = list(final_hypothesis['tracks'])
        tracks.sort(key=lambda x: x.decayed_count, reverse=True)

        for rank, track in enumerate(tracks):
            yield track, rank


