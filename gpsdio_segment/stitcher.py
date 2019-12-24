from __future__ import division, print_function, absolute_import
from collections import Counter
import datetime as DT
from itertools import chain
import logging
import math

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

log = logger.debug


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
    max_average_knots = 25 # fastest speed we allow when connecting segments
    # min_dist = 10 * 0.1 # uncertainty due to type 27 messages
    # penalty_hours = 24
    # # hours_exp = 2.0
    # buffer_hours = 1.0

    penalty_hours = 4
    hours_exp = 0.5
    buffer_hours = 0.25
    max_overlap_hours = 5 / 60

    # penalty_speed = 5.0

    # penalty_hours = 3
    # hours_exp = 0.0
    # buffer_hours = 0.25

    # shape_factor = 1.0

    # max_overlap_factor = 0.8
    # duration_weight = 0.1
    # overlap_weight = 1.0
    # speed_0 = 12.5
    min_sig_metric = 0.35
    # penalty_tracks = 4 # for more than this number of tracks become more strict
    #                     # todo: possibly only apply when multiple tracks.
    #                     # todo: possibly factor size in somehow
    #                     # penalty_hours = 12
    # hour_penalty = 2.0
    # speed_weight = 0.1
    lookahead = 32

    buffer_count = 10
    max_count_weight = 10.0

    max_active_tracks = 32
    # lookahead_penalty = 1.
    
    def __init__(self, **kwargs):
        for k in kwargs:
            # TODO: when interface stable, fix keys as in core.py
            self._update(k, kwargs)
    
    @staticmethod
    def aug_seg_id(obj):
        if obj['seg_id'] is None:
            return None
        return obj['seg_id'] + obj['timestamp'].date().isoformat()        

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
                {x['value'] : x['count'] for x in seg['transponders']},
                {x['value'] : x['count'] for x in seg['shipnames']},
                {x['value'] : x['count'] for x in seg['callsigns']},
                {x['value'] : x['count'] for x in seg['imos']},
            ]
        signatures = {}
        for seg in segs:
            sid = self.aug_seg_id(seg)
            signatures[sid] = get_sig(seg)
        return signatures
    
    def filter_and_sort(self, segs, min_seg_size=1):
        def is_null(x):
            return x is None or str(x) == 'NaT'
        def has_messages(s):
            return not(is_null(s['last_msg_of_day_timestamp']) and
                       is_null(s['first_msg_of_day_timestamp']))
        segs = [x for x in segs if has_messages(x)]
        segs.sort(key=lambda x: (x['timestamp'],x['first_msg_of_day_timestamp'], ))
        keys = ['shipnames', 'callsigns', 'imos', 'transponders']
        identities = {}
        sizes = {}
        days = {}
        for seg in segs:
            seg_id = seg['seg_id']
            sizes[seg_id] = max(seg['message_count'], sizes.get(seg_id, 0))
            if seg_id not in identities:
                identities[seg_id] = {k : {} for k in keys}
                days[seg_id] = 0
            days[seg_id] += 1
            for k in keys:
                for x in seg[k]:
                    v = x['value']
                    cnt = x['count']
                    identities[seg_id][k][v] = identities[seg_id][k].get(v, 0) + cnt

        for seg in segs:
            seg_id = seg['seg_id']
            assert seg_id is not None
            for k, sig in identities[seg_id].items():
                seg_sig = []
                for value, count in sig.items():
                    seg_sig.append({'value' : value, 'count' : count / days[seg_id]})
                seg[k] = seg_sig


        return [seg for seg in segs if sizes[seg['seg_id']] > min_seg_size]



    def compute_signature_metric(self, signatures, seg1, seg2):
        if seg1['seg_id'] == seg2['seg_id']:
            # These two chunks are from the same segment, so 
            # say they match
            return 1, True

        sig1 = signatures[self.aug_seg_id(seg1)][:2]
        sig2 = signatures[self.aug_seg_id(seg2)][:2]

        match = []
        for a, b in zip(sig1, sig2):
            if len(a) and len(b):
                na = math.sqrt(sum([v**2 for v in a.values()]))
                nb = math.sqrt(sum([v**2 for v in b.values()]))
                if na and nb:
                    maxa = max(a.values()) / na
                    maxb = max(b.values()) / nb
                    cos = 0
                    ta = 0 
                    tb = 0
                    for k in set(a) & set(b):
                        va = a[k] / na
                        vb = b[k] / nb
                        cos += va * vb
                        ta += va ** 2
                        tb += vb ** 2
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
                    match.append(cos2 * specificity)
        log('signature 1: %s', sig1)
        log('signature 2: %s', sig2)
        log('match_vector: %s', match)
        if len(match) == 0:
            return self.min_sig_metric, False            
        metric = 0.5 * (1 + min(match))
        if len(match) == 1:
            return min(metric, self.min_sig_metric), False
        return metric, (len(match) > 1)



    def overlaps(self, track, seg):
        def mostly_before(s0, s1):
            return self.timedelta(s0, s1).total_seconds() >= -self.max_overlap_hours / (60 * 60)
        for seg1 in track:
            if not (mostly_before(seg, seg1) or mostly_before(seg1, seg)):
                return  True
        return False

    @staticmethod
    def time_delta(seg0, seg1):
        return seg1['first_msg_of_day_timestamp'] - seg0['last_msg_of_day_timestamp']

    @staticmethod
    def _before(seg0, seg1):
        """True if seg0 is completely before seg1"""
        return Stitcher.time_delta(seg0, seg1).total_seconds() >= 0.0

    def track_index(self, track, seg):
        """Return index where seg would insert within track, raise Overlap if overlaps"""
        if not track:
            logging.info('track_index called on empty track')
            return 0
        before = self._before
        if before(seg, track[0]):
            logging.warning('track_index returning 0')
            return 0
        for i, segi in enumerate(track[:-1]):
            if not before(segi, seg):
                logger.info('internal overlap')
                raise Overlap
            if before(seg, track[i + 1]):
                logger.info('internal segment')
                return i + 1
        if not before(track[-1], seg):
            logger.info('internal overlap')
            raise Overlap
        return len(track)

    def _compute_metric(self, signatures, tgt, seg, signature_count):
        sig_metric, had_match = self.compute_signature_metric(signatures, seg, tgt)
        log('sig_metric: %s, had_match: %s', sig_metric, had_match)
        
        if sig_metric < self.min_sig_metric:
            if signature_count == 1:
                logger.info('sig_metric test failed when only one sig (%s, %s, %s',
                    signatures[self.aug_seg_id(seg)][:2], 
                    signatures[self.aug_seg_id(tgt)][:2],
                    sig_metric)
            log('failed signature test')
            logger.info("sig_metric: %s, min_sig_metric: %s", sig_metric, self.min_sig_metric)
            raise BadMatch()

        msg0 = {'timestamp' : tgt['last_msg_of_day_timestamp'], 
                'lat' : tgt['last_msg_of_day_lat'], 'lon' : tgt['last_msg_of_day_lon'],
                'speed' : tgt['last_msg_of_day_speed'], 'course' : tgt['last_msg_of_day_course']}
        msg1 = {'timestamp' : seg['first_msg_of_day_timestamp'], 
                'lat' : seg['first_msg_of_day_lat'], 'lon' : seg['first_msg_of_day_lon'],
                'speed' : seg['first_msg_of_day_speed'], 'course' : seg['first_msg_of_day_course']}
        overlapped = msg0['timestamp'] > msg1['timestamp']
        assert not overlapped

        hours = self.compute_msg_delta_hours(msg0, msg1)
        assert self.compute_msg_delta_hours(msg0, msg1) >= 0
        # effective_hours = math.hypot(hours, self.penalty_hours)
        penalized_hours = hours / (1 + (hours / self.penalty_hours) ** (1 - self.hours_exp))

        discrepancy = self.compute_discrepancy(msg0, msg1, penalized_hours)
        penalized_padded_hours = math.hypot(penalized_hours, self.buffer_hours)
        padded_hours = math.hypot(hours, self.buffer_hours)

        if tgt['seg_id'] == seg['seg_id']:
            speed_metric = 0
        else:
            speed = discrepancy / padded_hours
            # print(speed, self.max_average_knots, discrepancy, padded_hours)
            if speed > self.max_average_knots:
                logger.info('failed speed test')
                logger.info("discrepancy %s, effective_hours: %s, speed:  %s, max_speed: %s", discrepancy, padded_hours, speed, self.max_average_knots)
                raise BadMatch()
            # speed_metric = 1 - speed / self.max_average_knots  
            # print('metric', speed_metric)
            speed_metric = -discrepancy

        # print(discrepancy, speed_metric, sig_metric, speed, penalized_padded_hours, padded_hours)

        return sig_metric + speed_metric


    def compute_metric(self, signatures, track, seg, signature_count, track_counts):
        ndx = self.track_index(track, seg)
        assert ndx > 0, "should never be inserting before start of track ({})".format(ndx)
        track_id = self.aug_seg_id(track[0])
        reference = sorted(track_counts.values(), reverse=True)[:signature_count][-1]
        count = min(track_counts[track_id], reference)
        if reference > 0:
            alpha = count / reference
        else:
            alpha = 1

        count_metric = alpha * self.max_count_weight * math.log(self.buffer_count + count)
        if ndx == len(track):
            return ndx, self._compute_metric(signatures, track[ndx-1], seg, signature_count) + count_metric
        else:
            return ndx, 0.5 * (self._compute_metric(signatures, track[ndx-1], seg, signature_count) + 
                               self._compute_metric(signatures, seg, track[ndx], signature_count)) + count_metric


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
                for x in seg[k]:
                    v = x['value']
                    cnt = x['count']
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
        # TODO: add blank signatures when track has no active points
        end_range = start_date - DT.timedelta(days=1)
        start_range = start_date - DT.timedelta(days=lookback)
        segs = [x for x in segs if start_range <= x['timestamp'].date() <= end_range]
        seg_sigs = self._compute_signatures(segs)
        track_sigs = {}
        for track in tracks:
            track_id = self.aug_seg_id(track[0])
            sigs = [{}, {}, {}, {}]
            for seg in track:
                seg_id = self.aug_seg_id(seg)
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
            count = [x for x in track if start_range <= x['timestamp'].date() <= end_range]


    # def create_tracks(self, segs):
    def create_tracks(self, start_date, tracks, track_sigs, segs):
        # Trim tracks to start_date
        # Check that segs do not extend before start_date
        # Scan tracks and count number of tracks with > active_track_threshold (100 pts?) over active_track_window (30 days?)
        # 

        # TODO: determine "active_tracks" by looking at number of tracks in window with counts over threshold.
        # TODO: then add len(active_tracks) to max function below.

        signature_count = max(self.signatures_count(segs), 1)
        # For now just have two cases eventually, recalculate if
        # we later decide there are multiple signatures based on speed
        # also, perhaps be less agressive when we have more tracks than
        # signatures.
        # print('signature_count', signature_count)
        segs = self.filter_and_sort(segs, self.min_seg_size)
        signatures = self._compute_signatures(segs)

        signatures.update(track_sigs) # TODO: there is probably a lot of improving to do in the way we deal with signatures now
        for track in tracks:
            track_id = self.aug_seg_id(track[0])
            for seg in track:
                seg_id = self.aug_seg_id(seg)
                signatures[seg_id] = signatures[track_id]


        seg_source = iter(segs)
        active_segs = []
        while True:
            log('currently %s tracks', len(tracks))
            while len(active_segs) < self.lookahead:
                try:
                    # print("trying next", len(active_segs))
                    active_segs.append(next(seg_source))
                    # print("finished next", len(active_segs))
                except StopIteration:
                    # print("StopIteration", len(active_segs))
                    if len(active_segs) == 0:
                        logger.info("Created %s tracks from %s segments", len(tracks), len(segs))
                        return tracks
                    break

            track_counts = {}
            for track in tracks:
                track_id = Stitcher.aug_seg_id(track[0])
                sig = signatures[track_id]
                # TODO: This is an average count, could revamp to use real message count
                count = sum(sig[0].values()) #  Brittle :-()
                track_counts[track_id] = count

            active_tracks_ids = set(sorted(track_counts, key=lambda x: track_counts[x], 
                                        reverse=True)[:self.max_active_tracks])
            # print(len(tracks), len(active_segs), len(segs))
            best_track_info = None
            best_metric = -inf
            segs_with_match = set()
            for seg in active_segs:
                for track in tracks:
                    track_id = Stitcher.aug_seg_id(track[0])
                    if track_id not in active_tracks_ids:
                        continue
                    try:
                        ndx, metric = self.compute_metric(signatures, track, seg, signature_count, track_counts)
                    except Overlap: 
                        continue
                    except BadMatch:
                        continue
                    segs_with_match.add(self.aug_seg_id(seg))
                    if metric >= best_metric:
                        logger.info('new best metric: %s', metric)
                        best_metric = metric 
                        best_track_info = track, ndx, seg
            if best_track_info is not None: 
                best_track, ndx, best_seg = best_track_info
                # print('Adding to existing track', len(active_segs))
                match_ndx = active_segs.index(best_seg)
                active_segs.pop(match_ndx)
                # print(len(active_segs))
                new_sigs = []
                for s1, s2 in zip(signatures[self.aug_seg_id(best_seg)], 
                                  signatures[self.aug_seg_id(best_track[0])]):
                    new_sig = {}
                    for k in set(s1.keys()) | set(s2.keys()):
                        new_sig[k] = s1.get(k, 0) + s2.get(k, 0) 
                    new_sigs.append(new_sig)
                # print("inserting new seg at", ndx)
                best_track.insert(ndx, best_seg)
                for tgt in best_track:
                    signatures[self.aug_seg_id(tgt)] = new_sigs
                # If seg[0] did not match, turn it into it's own track
                if match_ndx != 0 and self.aug_seg_id(active_segs[0]) not in segs_with_match:
                    seg = active_segs.pop(0)
                    if seg['message_count'] >= self.min_seed_size:
                        logger.info('adding new track')
                        tracks.append([seg])
            else:
                # print('Not sdding to existing track', len(active_segs), len(tracks))
                while active_segs:
                    seg = active_segs.pop(0)
                    if seg['message_count'] >= self.min_seed_size:
                        logger.info('adding new track')
                        tracks.append([seg])
                        break
                # print(len(active_segs), len(tracks))
            # print(len(active_segs))
