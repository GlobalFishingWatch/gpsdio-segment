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
    buffer_hours = 5.0 / 60
    max_overlap_hours = 1
    max_overlap_points = 3
    max_overlap_fraction = 0.05
    speed_0 = 5
    min_sig_match = 0.5
    penalty_tracks = 4 # for more than this number of tracks become more strict
                        # todo: possibly only apply when multiple tracks.
                        # todo: possibly factor size in somehow
                        # penalty_hours = 12
    base_hour_penalty = 1.1
    no_id_hour_penalty = 2.0 # be more strict for tracks with no id info joining them across long times
    speed_weight = 10.
    
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
    
    @staticmethod
    def _extract_sig_part(msgs, inner_key, outer_key):
        counter = Counter()
        for m in msgs:
            for x in m[outer_key]:
                counter.update({x[inner_key] : x['cnt']})
        counted = counter.most_common(5)
        total = sum(count for (name, count) in counted)
        return dict([(name, count / total) for (name, count) in counted]), total

    @staticmethod
    def _update_sig_part(counters, msg, inner_key, outer):
        seg_id = msg['seg_id']
        if seg_id not in counters:
            counters[seg_id] = Counter()
        counter = counters[seg_id]
        for x in msg[outer_key]:
            counter.update({x[inner_key] : x['cnt']})
    
    def _compute_signatures(self, msgs, segs):
        msgs_by_seg = {}
        for msg in msgs:
            seg_id = msg['seg_id']
            if seg_id is None:
                continue
            if seg_id not in msgs_by_seg:
                msgs_by_seg[seg_id] = []
            msgs_by_seg[seg_id].append(msg)
        signatures = {}
        sig_counts = {}
        eroded_ends = {}
        # TODO: loop and build mapping of segids to [msgs] for each segment so we only traverse messages once
        for sid in msgs_by_seg:
            seg_msgs = msgs_by_seg[sid]
            # Compute eroded ends that ignore ends of segments in case there are a few bad points
            n = int(min(len(seg_msgs) *  self.max_overlap_fraction, self.max_overlap_points))
            eroded_ends[sid] = (seg_msgs[n]['timestamp'], seg_msgs[-(1 + n)]['timestamp'])
            # TODO: rework so that we traverse messages at most twice (once above and once here)
            # TODO: better yet traverse once
            a_types = set(['AIS.1', 'AIS.2', 'AIS.3'])
            b_types = set(['AIS.18', 'AIS.19'])
            ab_types = a_types | b_types
            a_cnt = b_cnt = tp_count = 0
            for msg in seg_msgs:
                a_cnt += msg['type'] in a_types
                b_cnt += msg['type'] in b_types
                tp_count += msg['type'] in ab_types
            if tp_count == 0:
                tp_type = {'is_A' : 0.5, 'is_B' : 0.5}
            else:
                tp_type = {'is_A' : a_cnt / tp_count, 'is_B' : b_cnt / tp_count}
            shipname, sn_count = self._extract_sig_part(seg_msgs, 'shipname', 'shipnames')
            callsign, cs_count = self._extract_sig_part(seg_msgs, 'callsign', 'callsigns')
            imo, imo_count = self._extract_sig_part(seg_msgs, 'imo', 'imos')
            signatures[sid] = (tp_type, shipname, callsign, imo)
            sig_counts[sid] = (tp_count, sn_count, cs_count, imo_count)
        return signatures, sig_counts, eroded_ends
    
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
            if len(a) and len(b):
                x = 0
                ta = 0 
                tb = 0
                for k in set(a) | set(b):
                    x += a.get(k, 0) * b.get(k, 0)
                    ta += a.get(k, 0) ** 2
                    tb += b.get(k, 0) ** 2
                wt = math.sqrt(ta * tb)
                match.append((x, wt))
        # print('>>>>')
        # print(sig1)
        # print(sig2)
        # print(match)
        if match:
            # The idea here is that we want to consider 
            #   (a) the worst match and
            #   (b) the most specific match
            # Minimizing x/wt minimizes a particular combination of
            # specificity and badness, then we multiply by weight to
            # get back the correct match value. There may be a cleaner
            # approach.
            x_wt, wt = min(((x / wt), wt) for (x, wt) in match)
            sig_metric = x_wt * wt
        else:
            sig_metric = self.min_sig_match
        # print(sig_metric)
        # print('<<<<')
        return sig_metric, bool(match)
    
    def create_tracks(self, msgs, segs):
        segs = self.filter_and_sort(segs)
        signatures, sig_counts, eroded_ends = self._compute_signatures(msgs, segs)
        # Build up tracks, joining to most reasonable segment (speed needed to join not crazy)
        tracks = []
        alive = True
        for seg in segs:
            best_track = None
            best_metric = 0
            for track in tracks:
                tgt = track[-1]
                seg_t0, seg_t1 = eroded_ends[seg['seg_id']]
                tgt_t0, tgt_t1 = eroded_ends[tgt['seg_id']]
                # TODO: rename to eroded_hours?
                eroded_hours = self.compute_ts_delta_hours(tgt_t1, seg_t0)
                dt1 = self.compute_ts_delta_hours(seg_t0, seg_t1)
                dt2 = self.compute_ts_delta_hours(tgt_t0, tgt_t1)
                max_overlap_hours = min(dt1 * self.max_overlap_fraction, 
                                        dt2 * self.max_overlap_fraction, self.max_overlap_hours)            

                if eroded_hours + max_overlap_hours <= 0:
                    continue 

                laxity = (1 if (len(tracks) < self.penalty_tracks) else 
                        math.sqrt(2) / math.hypot(1, len(tracks)/self.penalty_tracks))

                sig_metric, had_match = self.compute_signature_metric(signatures, seg, tgt)
                
                hours_exp = 1.0 / self.base_hour_penalty if had_match else 1.0 / self.no_id_hour_penalty
        
                
                if laxity * sig_metric < self.min_sig_match:
                    # print('failing match', laxity, sig_metric, self.min_sig_match)
                    continue
                # print('succesful match', laxity, sig_metric, self.min_sig_match)


                # TODO: use eroded msgs
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
                if metric > best_metric:

                    best_metric = metric 
                    best_track = track
            if best_track is not None: 
                new_sigs = []
                new_counts = []
                for s1, c1, s2, c2 in zip(signatures[seg['seg_id']], 
                                          sig_counts[seg['seg_id']],
                                          signatures[best_track[-1]['seg_id']], 
                                          sig_counts[best_track[-1]['seg_id']]):
                    tot = c1 + c2
                    new_sig = {}
                    for k in set(s1.keys()) | (s2.keys()):
                        n = s1.get(k, 0) * c1 + s2.get(k, 0) * c2
                        new_sig[k] = n / float(tot)
                    new_sigs.append(new_sig)
                    new_counts.append(tot)
                signatures[seg['seg_id']] = new_sigs
                sig_counts[seg['seg_id']] = new_counts
                best_track.append(seg)
            elif seg['message_count'] >= self.min_seed_size:
                tracks.append([seg])
            # print()
            # print()
                
        return tracks