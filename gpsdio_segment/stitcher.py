from __future__ import division, print_function, absolute_import
from collections import Counter
from itertools import chain
import math

from .discrepancy import DiscrepancyCalculator

class Stitcher(DiscrepancyCalculator):
    """Stitch segments together into coherent tracks.
    """
    
    min_seed_size = 20 # minimum segment size to start a track
    min_seg_size = 10 # segments shorter than this are dropped
    max_average_knots = 25 # fastest speed we allow when connecting segments
    # min_dist = 10 * 0.1 # uncertainty due to type 27 messages
    penalty_hours = 1
    # hours_exp = 2.0
    buffer_hours = 1.0
    max_overlap_hours = 1
    max_overlap_points = 3
    max_overlap_fraction = 0.1
    speed_0 = 12.5
    min_sig_match = 0.2
    penalty_tracks = 4 # for more than this number of tracks become more strict
                        # todo: possibly only apply when multiple tracks.
                        # todo: possibly factor size in somehow
                        # penalty_hours = 12
    base_hour_penalty = 1.5
    no_id_hour_penalty = 2.0 # be more strict for tracks with no id info joining them across long times
    speed_weight = 0.1
    
    def __init__(self, **kwargs):
        for k in kwargs:
            # TODO: when interface stable, fix keys as in core.py
            self._update(k, kwargs)
    
    @staticmethod
    def add_track_ids(msgs, tracks):
        # Use the seg_id of the first seg in the track as the track id.
        id_map = {}
        for track in tracks:
            track_id = track[0]['seg_id']
            for seg in track:
                id_map[seg['seg_id']] = track_id
        for msg in msgs:
            msg['track_id'] = id_map.get(msg['seg_id'], None)
    
    # @staticmethod
    # def _extract_sig_part(msgs, inner_key, outer_key):
    #     counter = Counter()
    #     for m in msgs:
    #         for x in m[outer_key]:
    #             counter.update({x[inner_key] : x['cnt']})
    #     counted = counter.most_common(5)
    #     total = sum(count for (name, count) in counted)
    #     return dict([(name, count / total) for (name, count) in counted]), total

    # @staticmethod
    # def _update_sig_part(counters, msg, inner_key, outer):
    #     seg_id = msg['seg_id']
    #     if seg_id not in counters:
    #         counters[seg_id] = Counter()
    #     counter = counters[seg_id]
    #     for x in msg[outer_key]:
    #         counter.update({x[inner_key] : x['cnt']})
    

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
            sid = seg['seg_id']
            signatures[sid] = get_sig(seg)
        return signatures
    # def _compute_signatures(self, msgs, segs):
    #     msgs_by_seg = {}
    #     for msg in msgs:
    #         seg_id = msg['seg_id']
    #         if seg_id is None:
    #             continue
    #         if seg_id not in msgs_by_seg:
    #             msgs_by_seg[seg_id] = []
    #         msgs_by_seg[seg_id].append(msg)
    #     signatures = {}
    #     sig_counts = {}
    #     eroded_ends = {}
    #     for sid in msgs_by_seg:
    #         seg_msgs = msgs_by_seg[sid]
    #         # Compute eroded ends that ignore ends of segments in case there are a few bad points
    #         n = int(min(len(seg_msgs) *  self.max_overlap_fraction, self.max_overlap_points))
    #         eroded_ends[sid] = (seg_msgs[n]['timestamp'], seg_msgs[-(1 + n)]['timestamp'])
    #         a_types = set(['AIS.1', 'AIS.2', 'AIS.3'])
    #         b_types = set(['AIS.18', 'AIS.19'])
    #         ab_types = a_types | b_types
    #         a_cnt = b_cnt = tp_count = 0
    #         for msg in seg_msgs:
    #             a_cnt += msg['type'] in a_types
    #             b_cnt += msg['type'] in b_types
    #             tp_count += msg['type'] in ab_types
    #         if tp_count == 0:
    #             tp_type = {'is_A' : 0.5, 'is_B' : 0.5}
    #         else:
    #             tp_type = {'is_A' : a_cnt / tp_count, 'is_B' : b_cnt / tp_count}
    #         shipname, sn_count = self._extract_sig_part(seg_msgs, 'shipname', 'shipnames')
    #         callsign, cs_count = self._extract_sig_part(seg_msgs, 'callsign', 'callsigns')
    #         imo, imo_count = self._extract_sig_part(seg_msgs, 'imo', 'imos')
    #         signatures[sid] = (tp_type, shipname, callsign, imo)
    #         sig_counts[sid] = (tp_count, sn_count, cs_count, imo_count)
    #     return signatures, sig_counts, eroded_ends
    
    def filter_and_sort(self, segs):
        segs = [seg for seg in segs if seg['seg_id'] is not None 
                               and seg['message_count'] >= self.min_seg_size]
        segs.sort(key=lambda x: x['first_timestamp'])
        return segs
    
    def compute_signature_metric(self, signatures, seg1, seg2):
        sig1 = signatures[seg1['seg_id']]
        sig2 = signatures[seg2['seg_id']]

        # TODO: match should treat / B signature differently than others since that should 
        # Almost always be present, and is 50/50 so is a much weaker signal. Primarily a negative
        # signal if A / B mismatch then it's likely different tracks, but if they do then who knows
        
        # TODO: leverage wts to return weight that influences how seriously we take this. Note note
        #       about A/B above too.
        match = []
        for a, b in zip(sig1, sig2):
            # TODO: If a metric is present in one and not the other, it's not a definitive signal
            # since short segments may not get any identity messages (sometimes only type 27 are
            # received.) So, we want to consider 3 cases:
            # 1. No data present in one or both sigs: return self.min_sig_match (rare)
            # 2. Only A/B data in one or both sigs: return min(sig_metric, self.min_sig_match)
            # 3. General case.
            if len(a) and len(b):
                x = 0
                ta = 0 
                tb = 0
                na = sum(a.values())
                nb = sum(b.values())
                for k in set(a) & set(b):
                    va = a[k] / na
                    vb = b[k] / nb
                    x += va * vb
                    ta += va ** 2
                    tb += vb ** 2
                wt = math.sqrt(ta * tb) + 1e-10
                match.append((x, wt))
        # print('>>>>')
        # print(sig1)
        # print(sig2)
        # print(match)
        if len(match) == 0:
            return self.min_sig_match, False
        elif len(match) == 1:
            # TODO: fix...
            sig_metric = match[0][0]
            return min(sig_metric, self.min_sig_match), False
        else:
            # The idea here is that we want to consider 
            #   (a) the worst match and
            #   (b) the most specific match
            # Minimizing x/wt minimizes a particular combination of
            # specificity and badness, then we multiply by weight to
            # get back the correct match value. There may be a cleaner
            # approach.
            x_wt, wt = min(((x / wt), wt) for (x, wt) in match)
            return x_wt * wt, True

    
    def create_tracks(self, segs):
        segs = self.filter_and_sort(segs)
        signatures = self._compute_signatures(segs)
        # Build up tracks, joining to most reasonable segment (speed needed to join not crazy)
        tracks = []
        alive = True
        for seg in segs:
            # print('\n\n')
            best_track = None
            best_metric = 0
            for track in tracks:
                tgt = track[-1]
                seg_t0, seg_t1 = seg['first_timestamp'], seg['last_timestamp']
                tgt_t0, tgt_t1 = tgt['first_timestamp'], tgt['last_timestamp']
                delta_hours = self.compute_ts_delta_hours(tgt_t1, seg_t0)
                dt1 = self.compute_ts_delta_hours(seg_t0, seg_t1)
                dt2 = self.compute_ts_delta_hours(tgt_t0, tgt_t1)
                max_overlap_hours = min(dt1 * self.max_overlap_fraction, 
                                        dt2 * self.max_overlap_fraction)  

                if delta_hours + max_overlap_hours <= 0:
                    # print('Hours', delta_hours, max_overlap_hours)
                    continue 

                laxity = (1 if (len(tracks) < self.penalty_tracks) else 
                        math.sqrt(2) / math.hypot(1, len(tracks)/self.penalty_tracks))

                sig_metric, had_match = self.compute_signature_metric(signatures, seg, tgt)

                # print('sig_metric', sig_metric)
                
                hours_exp = 1.0 / self.base_hour_penalty if had_match else 1.0 / self.no_id_hour_penalty
        
                
                if laxity * sig_metric < self.min_sig_match:
                    # print('failing match', laxity, sig_metric, self.min_sig_match)
                    continue
                # print('succesful match', laxity, sig_metric, self.min_sig_match)


                msg0 = {'timestamp' : tgt['last_timestamp'], 
                        'lat' : tgt['last_lat'], 'lon' : tgt['last_lon'],
                        'speed' : tgt['last_speed'], 'course' : tgt['last_course']}
                msg1 = {'timestamp' : seg['first_timestamp'], 
                        'lat' : seg['first_lat'], 'lon' : seg['first_lon'],
                        'speed' : seg['first_speed'], 'course' : seg['first_course']}
                if tgt['last_timestamp'] > seg['first_timestamp']:
                    # Segments overlap so flip the messages to keep time positive
                    msg0, msg1 = msg1, msg0


                hours = math.hypot(self.compute_msg_delta_hours(msg0, msg1), self.buffer_hours)
                effective_hours =(hours if (hours < self.penalty_hours) else 
                                  hours * (hours / self.penalty_hours) ** hours_exp)
                discrepancy = self.compute_discrepancy(msg0, msg1)

                speed = discrepancy / effective_hours # TODO: use segmentizer hours?
                if speed > self.max_average_knots * laxity:
                    # print('failing speed match', speed, laxity, effective_hours)
                    continue

                speed_metric = math.exp(-(speed / self.speed_0) ** 2) / hours    
                metric = sig_metric + self.speed_weight * speed_metric
                # print(sig_metric, speed_metric, metric)
                if metric > best_metric:
                    best_metric = metric 
                    best_track = track
            if best_track is not None: 
                new_sigs = []
                new_counts = []
                for s1, s2 in zip(signatures[seg['seg_id']], 
                                          signatures[best_track[-1]['seg_id']]):
                    new_sig = {}
                    for k in set(s1.keys()) | set(s2.keys()):
                        new_sig[k] = s1.get(k, 0) + s2.get(k, 0) 
                    new_sigs.append(new_sig)
                signatures[seg['seg_id']] = new_sigs
                best_track.append(seg)
            elif seg['message_count'] >= self.min_seed_size:
                tracks.append([seg])
            # print()
            # print()
                
        return tracks