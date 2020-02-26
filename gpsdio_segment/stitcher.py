from __future__ import division, print_function, absolute_import
from collections import Counter, namedtuple
import datetime as DT
from itertools import chain
import logging
import math

Track = namedtuple('Track', ['id', 'prefix', 'segments'])

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

log = logger.debug

EPSILON = 1e-10

def clip(x, l, h):
    if x < l:
        return l
    if x > h:
        return h
    return x

epsilon = 1e-6

from .discrepancy import DiscrepancyCalculator

class Overlap(Exception):
    pass

class BadMatch(Exception):
    pass

inf = float('Inf')

class Stitcher(DiscrepancyCalculator):
    """Stitch segments together into coherent tracks.
    """
    
    min_seed_size = 5 # minimum segment size to start a track
    min_seg_size = 3 # segments shorter than this are dropped
    max_average_knots = 50 # fastest speed we allow when connecting segments

    penalty_hours = 4
    hours_exp = 0.5
    buffer_hours = 1.0
    max_overlap_hours = 2.0
    max_overlap_fraction = max_overlap_hours / 24.0
    min_sig_metric = 0.35
    max_lookahead = 32
    buffer_count = 10
    max_count_weight = 10.0
    max_active_tracks = 8

    signature_weight = 1.0
    discrepancy_weight = 10.0
    overlap_weight = .0
    speed_weight = 10.0
    time_metric_weight = 1.0
    max_discrepancy = 1500
    time_metric_scale_hours = 7 * 24
    same_seg_weight = 0.2
    
    def __init__(self, **kwargs):
        for k in kwargs:
            # TODO: when interface stable, fix keys as in core.py
            self._update(k, kwargs)
    
    @staticmethod
    def aug_seg_id(dict_obj):
        if dict_obj['seg_id'] is None:
            return None
        return dict_obj['seg_id'] + '-' + dict_obj['timestamp'].date().isoformat()        

    @staticmethod
    def add_track_ids(msgs, tracks):
        # Use the seg_id of the first seg in the track as the track id.
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



    def compute_signature_metric(self, signatures, seg1, seg2):
        if seg1.id == seg2.id:
            # These two chunks are from the same segment, so 
            # say they match
            return 1, True

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
        return 2 * raw_metric - 1, True
      


    def _mostly_before(self, s0, s1, tolerance=0):
        def dt(s):
            return (s.last_msg_of_day.timestamp - 
                    s.first_msg_of_day.timestamp).total_seconds() / (60 * 60)
        dt0 = dt(s0)
        dt1 = dt(s1)
        max_oh = min(self.max_overlap_hours, 
                     self.max_overlap_fraction * dt0,
                     self.max_overlap_fraction * dt1)
        return self.time_delta(s0, s1).total_seconds() / (60 * 60) >= -max_oh * (1 - tolerance)

    @staticmethod
    def time_delta(seg0, seg1):
        return seg1.first_msg_of_day.timestamp - seg0.last_msg_of_day.timestamp

    def track_index(self, track, seg, tolerance):
        """Return index where seg would insert within track, raise Overlap if overlaps"""
        if not track:
            logging.info('track_index called on empty track')
            return 0
        def mostly_before(s0, s1):
            return self._mostly_before(s0, s1, tolerance)
        if mostly_before(seg, track[0]):
            logging.warning('track_index returning 0')
            return 0
        for i, _ in enumerate(track[:-1]):
            if not mostly_before(track[i], seg):
                assert track[i].id != seg.id
                logger.info('internal overlap %s', self.time_delta(track[0], seg).total_seconds() / (60 * 60))
                raise Overlap
            if mostly_before(seg, track[i + 1]):
                logger.debug('internal segment')
                return i + 1
        if not mostly_before(track[-1], seg):
            logger.info('internal overlap %s', self.time_delta(track[-1], seg).total_seconds() / (60 * 60))
            raise Overlap
        return len(track)

    def _tolerance(self, signature_count, n_tracks):
        # Tolerance *for new tracks*, so when this is lower we should be less
        # likely to form new tracks
        alpha = (n_tracks + 1) / max(signature_count, 1)
        # print(n_tracks, signature_count, alpha, math.exp(-alpha))
        return math.exp(-alpha)

    def _compute_metric(self, tgt, seg, hard_fail=True):
        n_tracks = len(self.track_counts)
        signatures = self.signatures
        sig_metric, had_match = self.compute_signature_metric(signatures, seg, tgt)
        log('sig_metric: %s, had_match: %s', sig_metric, had_match)
        # Adjust the tolerance to generating new tracks based on  the number of 
        # excess tracks present.
        tolerance = self._tolerance(self.signature_count, n_tracks)
        # TODO: think about incorporating tolerance into sig_metric
        if hard_fail and sig_metric < self.min_sig_metric:
            if self.signature_count == 1:
                logger.info('sig_metric test failed when only one sig (%s, %s, %s %s)',
                    signatures[seg.aug_id][:2], 
                    signatures[tgt.aug_id][:2],
                    sig_metric, tolerance)
            log('failed signature test')
            logger.info("sig_metric: %s, min_sig_metric: %s", sig_metric, self.min_sig_metric)
            raise BadMatch()

        msg0 = tgt.last_msg_of_day
        msg1 = seg.first_msg_of_day
        overlapped = msg0.timestamp > msg1.timestamp
        if overlapped:
            msg0, msg1 = msg1, msg0
            def dt(s):
                return (s.last_msg_of_day.timestamp - 
                        s.first_msg_of_day.timestamp).total_seconds() / (60 * 60)
            dt0 = dt(seg)
            dt1 = dt(tgt)
            max_oh = min(self.max_overlap_hours, 
                         self.max_overlap_fraction * dt0,
                         self.max_overlap_fraction * dt1)
            oh = self.compute_msg_delta_hours(msg0._asdict(), msg1._asdict()) 
            overlap_metric = 1 - oh / max_oh
        else:
            overlap_metric = 1




        hours = self.compute_msg_delta_hours(msg0._asdict(), msg1._asdict())
        assert hours >= 0
        penalized_hours = hours / (1 + (hours / self.penalty_hours) ** (1 - self.hours_exp))

        discrepancy = self.compute_discrepancy(msg0._asdict(), msg1._asdict(), penalized_hours)
        penalized_padded_hours = math.hypot(penalized_hours, self.buffer_hours)
        padded_hours = math.hypot(hours, self.buffer_hours)


        if tgt.id == seg.id:
            disc_metric = 1
        else:
            if hard_fail and discrepancy > self.max_discrepancy:
                logger.info('failed discrepancy test')
                logger.info("discrepancy %s, max_discrepancy", 
                             discrepancy, self.max_discrepancy)
                raise BadMatch()   
            disc_metric = 1 - discrepancy / self.max_discrepancy

        speed = discrepancy / padded_hours
        speed_metric = 1 - speed / ((1 - tolerance) * self.max_average_knots)
        if hard_fail and speed_metric < 0:
            logger.info('failed speed test')
            logger.info("discrepancy %s, effective_hours: %s, speed:  %s, max_speed: %s", 
                         discrepancy, padded_hours, speed, self.max_average_knots)
            raise BadMatch()


        time_metric = self.time_metric_scale_hours / (self.time_metric_scale_hours + hours) 
        # TODO: time_metric

        id1 = tgt.id
        id2 = seg.id
        same_seg = (id1 == id2)

        return ( self.signature_weight * sig_metric +
                 self.discrepancy_weight * disc_metric +
                 self.speed_weight * speed_metric +
                 self.time_metric_weight * time_metric +
                 self.overlap_weight * overlap_metric + 
                 self.same_seg_weight * same_seg)


    def compute_metric(self, track, seg):
        track_counts = self.track_counts
        n_tracks = len(track_counts)
        tolerance = self._tolerance(self.signature_count, n_tracks)
        ndx = self.track_index(track, seg, tolerance)
        assert ndx > 0, "should never be inserting before start of track ({})".format(ndx)
        track_id = track[0].aug_id
        reference = max(track_counts.values())
        count = track_counts.get(track_id, 0) # TODO: do something sensible for hypothoses
        count_metric = self.max_count_weight * count / (reference + self.buffer_count)
        if ndx == len(track):
            return ndx, self._compute_metric(track[ndx-1], seg) + count_metric
        else:
            return ndx, 0.5 * (self._compute_metric(track[ndx-1], seg) + 
                               self._compute_metric(seg, track[ndx])) + count_metric

    @staticmethod
    def signatures_count(segs, sig_abs=1, sig_frac=0.05):
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


    def find_track_signatures(self, start_date, tracks, segs, lookback=30):
        end_range = start_date - DT.timedelta(days=1)
        start_range = start_date - DT.timedelta(days=lookback)
        segs = [x for x in segs if start_range <= x.timestamp <= end_range]
        seg_sigs = self._compute_signatures(segs)
        track_sigs = {}
        for track in tracks:
            track_id = track[0].aug_id
            sigs = [{}, {}, {}, {}]
            for seg in track:
                seg_id = seg.aug_id
                if seg_id in seg_sigs:
                    track_sigs
                    for i, (s1, s2) in enumerate(zip(seg_sigs[seg_id], sigs)):
                        for k in set(s1.keys()) | set(s2.keys()):
                            sigs[i][k] = s1.get(k, 0) + s2.get(k, 0) 
            track_sigs[track_id] = sigs

        return track_sigs


    def active_tracks(self, start_date, tracks, lookback=30):
        end_range = start_date - DT.timedelta(days=1)
        start_range = start_date - DT.timedelta(days=lookback)
        active = []
        for track in tracks:
            count = [x for x in track if start_range <= x.timestamp.date() <= end_range]


    # def create_tracks(self, segs):
    def create_tracks(self, start_date, tracks, track_sigs, segs):
        # Trim tracks to start_date
        # Check that segs do not extend before start_date
        # Scan tracks and count number of tracks with > active_track_threshold (100 pts?) over active_track_window (30 days?)
        # 
        self.signature_count = max(self.signatures_count(segs), 1)
        # For now just have two cases eventually, recalculate if
        # we later decide there are multiple signatures based on speed
        # also, perhaps be less agressive when we have more tracks than
        # signatures.
        # print('signature_count', signature_count)
        segs = self.filter_and_sort(segs, self.min_seg_size)
        self.signatures = signatures = self._compute_signatures(segs)

        signatures.update(track_sigs)
        for track in tracks:
            track_id = track[0].aug_id
            for seg in track:
                seg_id = seg.aug_id
                signatures[seg_id] = signatures[track_id]

        # Now remove all segments that occur before today:
        segs = [x for x in segs if x.timestamp.date() >= start_date.date()]

        seg_source = iter(segs)
        active_segs = []
        while True:
            log('currently %s tracks', len(tracks))
            self.track_counts = track_counts = {}
            for track in tracks:
                track_id = track[0].aug_id
                sig = signatures[track_id]
                # TODO: This is an average count, could revamp to use real message count
                count = sum(sig[0].values()) #  Brittle :-()
                track_counts[track_id] = count

            track_ids_by_count= sorted(track_counts, key=lambda x: track_counts[x], reverse=True)
            active_tracks_ids = set(sorted(track_counts, key=lambda x: track_counts[x], 
                                           reverse=True)[:self.max_active_tracks])

            while len(active_segs) < self.max_lookahead:
                try:
                    active_segs.append(next(seg_source))
                except StopIteration:
                    if len(active_segs) == 0:
                        logger.info("Created %s tracks from %s segments", len(tracks), len(segs))
                        for track in tracks:
                            track_id = track[0].aug_id
                            ndx = track_ids_by_count.index(track_id)
                            yield track, ndx, 'active'
                        return
                    break

            # Inactive tracks are never resurrected, so emit now.
            active_tracks = []
            for track in tracks:
                track_id = track[0].aug_id
                ndx = track_ids_by_count.index(track_id)
                if track_id in active_tracks_ids:
                    active_tracks.append(track)
                else:
                    yield track, ndx, 'inactive'
            tracks = active_tracks
            best_track_info = None
            best_metric = -inf
            segs_with_match = set()

            for seg in active_segs:
                for track in tracks:
                    track_id = track[0].aug_id
                    try:
                        ndx, metric = self.compute_metric(track, seg)
                    except Overlap: 
                        continue
                    except BadMatch:
                        continue
                    segs_with_match.add(seg.aug_id)
                    if metric >= best_metric:
                        assert metric is not None
                        logger.debug('new best metric: %s', metric)
                        best_metric = metric 
                        best_track_info = track, ndx, seg
            if best_track_info is not None: 
                best_track, ndx, best_seg = best_track_info
                match_ndx = active_segs.index(best_seg)
                assert active_segs.pop(match_ndx) == best_seg
                new_sigs = []
                for s1, s2 in zip(signatures[best_seg.aug_id], 
                                  signatures[best_track[0].aug_id]):
                    new_sig = {}
                    for k in set(s1.keys()) | set(s2.keys()):
                        new_sig[k] = s1.get(k, 0) + s2.get(k, 0) 
                    new_sigs.append(new_sig)
                best_track.insert(ndx, best_seg)
                for tgt in best_track:
                    signatures[tgt.aug_id] = new_sigs
                # If seg[0] did not match due to overlap or speed, turn it into it's own track
                # This is so that it's a candidate to add to for other tracks and since it
                # is unlikely be added to any existing track (technically, it could get added
                # if it failed due to speed when another segment gets added, but it's unlikely)
                if match_ndx != 0 and active_segs[0].aug_id not in segs_with_match:
                    seg = active_segs.pop(0)
                    if seg.msg_count >= self.min_seed_size:
                        logger.info('adding new track')
                        tracks.append([seg])
            else:
                while active_segs:
                    seg = active_segs.pop(0)
                    if seg.msg_count >= self.min_seed_size:
                        logger.info('adding new track')
                        tracks.append([seg])
                        break

    # Extra experimental below here



    def update_hypotheses(self, hypotheses, segment):
        # TODO: replace seg_ids, with segments implemented as namedtuples
        updated = []
        for track_list in hypotheses:
            for i, track in enumerate(track_list):
                new_list = list(track_list)
                new_list[i] = track._replace(segments=track.segments + (segment,))
                updated.append(new_list)
            new_list = list(track_list)
            new_list.append(Track(id=segment.id, prefix=[], segments=(segment,)))
            updated.append(new_list)
        return updated

    _seg_joining_costs = {}

    @property
    def base_track_cost(self):
        return ( self.signature_weight +
                 self.discrepancy_weight +
                 self.time_metric_weight +
                 self.same_seg_weight +
                 self.speed_weight + 
                 self.overlap_weight
                )    

    # def compute_cost(self, seg0, seg1):
        # Implement directly

    def compute_cost(self, seg0, seg1):
        return self.base_track_cost - self._compute_metric(seg0, seg1, hard_fail=False)

    def find_cost(self, seg0, seg1):
        key = (seg0, seg1)
        if key not in self._seg_joining_costs:
            self._seg_joining_costs[key] = self.compute_cost(seg0, seg1)
        return self._seg_joining_costs[key]

    def compute_track_count(self, track):
        cnt = 0
        for seg in track.prefix:
            cnt += seg.msg_count
        for seg in track.segments:
            cnt += seg.msg_count
        return cnt

    def track_cost(self, track, max_count):

        cost = 0.5 * self.base_track_cost 

        if track.prefix:
            seg0 = track.prefix[-1]
            segments = track.segments
        elif track.segments:
            seg0 = track.segments[0]
            segments = track.segments[1:]
        else:
            return cost

        for seg1 in segments:
            cost += self.find_cost(seg0, seg1)
            seg0 = seg1
        return cost

    def hypothesis_cost(self, hypothesis, max_count):

        track_counts = [self.compute_track_count(track) for track in hypothesis]
        countsum2 = sum(track_counts) ** 2
        veclen2 = sum(x**2 for x in track_counts)
        length_cost = 0#self.max_count_weight * (1 - veclen2 / countsum2)

        # # TODO: think this could be made to range 0-1 instead of almost 1
        # count_metric = self.max_count_weight * count / (max_track_count + self.buffer_count)
        # count_cost = self.max_count_weight - count_metric

        return sum(self.track_cost(x, max_count) for x in hypothesis) + length_cost

    def prune_hypotheses(self, hypotheses_list, n):

        max_track_count = max(max(self.compute_track_count(track) for track in hypothesis)
            for hypothesis in hypotheses_list)

        # TODO: want to favor long tracks over short tracks, so incremental cost of adding 
        # elements goes down Maybe charge Sum(ar^k) = a / (1 - r), but fix for truncation.
        # For N terms (0 to N - 1) =  a / (1 - r) - ar**N / (1 - r) = a * (1 + r**N) / (1 - r)

        if len(hypotheses_list) > n:
            costs = [self.hypothesis_cost(x, max_track_count) for x in hypotheses_list]
            ndxs = list(range(len(hypotheses_list)))
            ndxs.sort(key=lambda i: costs[i])
            hypotheses_list = [hypotheses_list[i] for i in ndxs[:n]]
        return hypotheses_list

    def condense_hypotheses(self, hypotheses_list):
        """
        If the same prefix occurs in all tracks it can be removed.
        """
        if len(hypotheses_list) == 0:
            return hypotheses_list

        prefixes = {}
        base_prefixes = {}
        for track in hypotheses_list[0]:
            prefixes[track.id] = set(enumerate(track.segments))
            base_prefixes[track.id] = track.prefix

        for hypothesis in hypotheses_list[1:]:
            for track in hypothesis:
                if track.id not in prefixes:
                    prefixes[track.id] = set()
                prefixes[track.id] = prefixes[track.id] & set(enumerate(track.segments))
                assert track.prefix == base_prefixes[track.id]

        prefix_indices = {}

        for id_, seg_set in prefixes.items():
            segs_list = sorted(seg_set)
            for i, (ndx, seg) in enumerate(segs_list):
                if i != ndx:
                    prefix_indices[id_] = i
                    break
            else:
                prefix_indices[id_] = len(segs_list)

        new_hypotheses_list = []
        for hypothesis in hypotheses_list:
            new_hypothesis = []
            for track in hypothesis:
                ndx = prefix_indices[track.id]
                new_hypothesis.append(Track(track.id, 
                                            tuple(track.prefix) + tuple(track.segments[:ndx]),
                                            track.segments[ndx:]))
            new_hypotheses_list.append(new_hypothesis)

        return new_hypotheses_list


    def create_tracks_stub(self, start_date, tracks, track_sigs, segs):
         # Trim tracks to start_date
        # Check that segs do not extend before start_date
        # Scan tracks and count number of tracks with > active_track_threshold (100 pts?) over active_track_window (30 days?)
        # 
        self.signature_count = max(self.signatures_count(segs), 1)
        # For now just have two cases eventually, recalculate if
        # we later decide there are multiple signatures based on speed
        # also, perhaps be less agressive when we have more tracks than
        # signatures.
        # print('signature_count', signature_count)
        segs = self.filter_and_sort(segs, self.min_seg_size)
        self.signatures = signatures = self._compute_signatures(segs)

        signatures.update(track_sigs)
        for track in tracks:
            track_id = track[0].aug_id
            for seg in track:
                seg_id = seg.aug_id
                signatures[seg_id] = signatures[track_id]

        # Now remove all segments that occur before today:
        segs = [x for x in segs if x.timestamp.date() >= start_date.date()]

        seg_source = iter(segs)
        active_segs = []

        self.track_counts = track_counts = {}

        # New below here
        max_hypotheses = 64

        assert tracks == [] # TODO relax this and construct initial hypotheses 
        hypotheses = [[]]
        for seg in segs:
            hypotheses = self.update_hypotheses(hypotheses, seg)
            hypotheses = self.prune_hypotheses(hypotheses, max_hypotheses)
            hypotheses = self.condense_hypotheses(hypotheses)
        final_hypothesis = self.prune_hypotheses(hypotheses, 1)[0]

        return final_hypothesis

    @staticmethod
    def add_track_ids_2(msgs, tracks):
        # Use the seg_id of the first seg in the track as the track id.
        msgs = msgs.to_dict('records')
        id_map = {}
        for track in tracks:
            track_id = track.id
            for seg in track.prefix:
                id_map[seg.aug_id] = track_id
            for seg in track.segments:
                id_map[seg.aug_id] = track_id
        for msg in msgs:
            try:
                msg['track_id'] = id_map.get(Stitcher.aug_seg_id(msg), None)
            except:
                print(msg)
                raise

        import pandas as pd
        return pd.DataFrame(msgs)
