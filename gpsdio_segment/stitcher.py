from __future__ import division, print_function, absolute_import
from collections import namedtuple, Counter
import datetime as DT
import logging
import math

from .discrepancy import DiscrepancyCalculator

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

Track = namedtuple('Track', ['id', 'seg_ids', 'count', 'decayed_count', 'is_active',
                             'signature', 'parent_track', 'last_msg'])

Signature = namedtuple('Signature', ['transponders', 'shipnames', 'callsigns', 'imos',
                    'destinations', 'lengths', 'widths'])


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
    max_hypotheses = 20
    min_seg_size = 3 # segments shorter than this are dropped
    max_active_tracks = 10

    # Parameters controlling how effective hours are computed
    penalty_hours = 4
    hours_exp = 0.5
    buffer_hours = 0.1

    # Parameters used in computing costs
    buffer_count = 10
    max_discrepancy = 2000
    time_metric_scale_hours = 14 * 24
    max_average_knots = 30 
    max_overlap_hours = 12.0
    max_overlap_fraction = max_overlap_hours / 24.0
    base_track_cost = 4.0
    base_count = 20.0

    # Weights of various cost components
    count_weight = 1.0
    signature_weight = 1.0
    discrepancy_weight = 1.0
    overlap_weight = 1.0
    speed_weight = 1.0
    time_metric_weight = 1.0

    decay_per_day = 0.99

    sig_specificity_fraction = 0.1
    sig_specificity_abs = 10

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
            track_id = track['track_id']
            for seg_id in track['seg_ids']:
                id_map[seg_id] = track_id
        for msg in msgs:
            try:
                msg['track_id'] = id_map.get(Stitcher.aug_seg_id(msg), None)
            except:
                print(msg)
                raise

    @staticmethod
    def get_seg_sig(seg):
        return Signature(
            transponders = dict(seg.transponders),
            shipnames = dict(seg.shipnames),
            callsigns = dict(seg.callsigns),
            imos = dict(seg.imos),
            destinations = dict(seg.destinations),
            lengths = dict(seg.lengths),
            widths = dict(seg.widths)
        )
    
    def filter_and_sort(self, segs, min_seg_size, start_date):
        def is_null(x):
            return x is None or str(x) == 'NaT'
        def has_messages(s):
            return not(is_null(s.last_msg_of_day.timestamp) and
                       is_null(s.first_msg_of_day.timestamp))
        segs = [x for x in segs if has_messages(x) and 
                                   x.timestamp.date() >= start_date.date() and
                                   x.daily_msg_count  >= self.min_seg_size]
        segs.sort(key=lambda x: (x.timestamp, x.last_msg_of_day.timestamp, x.id))
        return segs


    @staticmethod
    def seg_duration(seg):
        return (seg.last_msg_of_day.timestamp - 
                seg.first_msg_of_day.timestamp)

    def signature_cost(self, track, seg):
        sig1 = track.signature
        sig2 = self.get_seg_sig(seg)
        # Cap positive specificities based on global occurrences of different sig values.
        # transponder values can only have negative values, since there aren't enough
        # options to be positively specific
        max_pos_specificities = [0, 1, 1, 1, 1, 1, 1]

        match = []
        for a, b, mps in zip(sig1, sig2, max_pos_specificities):
            if len(a) and len(b):
                # Protect against overflow
                max_a = max(abs(x) for x in a.values())
                max_b = max(abs(x) for x in b.values()) 
                if max_a > EPSILON and max_b > EPSILON:
                    a_scale = max(max_a, 1)
                    b_scale = max(max_b, 1)
                    scaled_a = {k : v / a_scale for (k, v) in a.items()}
                    scaled_b = {k : v / b_scale for (k, v) in b.items()}

                    na = math.sqrt(sum([(v)**2 for v in scaled_a.values()])) 
                    nb = math.sqrt(sum([(v)**2 for v in scaled_b.values()])) 
                    if na < EPSILON or nb < EPSILON:
                        continue
                    # TODO: rename maxa maxb to something reflecting their relative nature
                    maxa = max(scaled_a.values()) / na
                    maxb = max(scaled_b.values()) / nb
                    cos = 0
                    for k in set(a) & set(b):
                        va = scaled_a[k] / na
                        vb = scaled_b[k] / nb
                        cos += va * vb
                    try:
                        cos2 = 2 * cos ** 2 - 1
                    except:
                        cos2 = 0
                        logger.warning('cos2 computation failed using 0 ({}) ({})'.format(a, b))
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


        raw_metric = sum(x[0] * x[1] for x in match) / (sum(x[1] for x in match) + EPSILON)

        # # Raw metric is a value between -1 and 1
        # raw_metric = sum((x[0] * x[1]) for x in match) / len(match)
        # Return a value between 0 and 1
        return self.signature_weight * 0.5 * (1 - raw_metric)
      

    def overlap_cost(self, last_msg1, seg2):
        msg0 = last_msg1
        msg1 = seg2.first_msg_of_day
        overlapped = msg0.timestamp > msg1.timestamp
        if overlapped:
            msg0, msg1 = msg1, msg0
            def dt(s):
                return (s.last_msg_of_day.timestamp - 
                        s.first_msg_of_day.timestamp).total_seconds() / S_PER_HR
            dt1 = self.seg_duration(seg2).total_seconds() / S_PER_HR
            max_oh = min(self.max_overlap_hours, 
                         self.max_overlap_fraction * dt1)
            oh = self.compute_msg_delta_hours(msg0._asdict(), msg1._asdict()) 
            return self.overlap_weight * oh / max_oh
        else:
            return 0



    def compute_cost(self, last_msg1, seg2):
        overlap_cost = self.overlap_cost(last_msg1, seg2)

        hours = (seg2.first_msg_of_day.timestamp - last_msg1.timestamp).total_seconds() / S_PER_HR
        msg1 = last_msg1
        msg2 = seg2.first_msg_of_day
        if hours < 0:
            # Overlap is already penalized, so swap seg1 and seg2
            msg1, msg2 = msg2, msg1
            hours = -hours
        penalized_hours = hours / (1 + (hours / self.penalty_hours) ** (1 - self.hours_exp))
        discrepancy, dist = self.compute_discrepancy_and_dist(
                                msg1._asdict(), msg2._asdict(), penalized_hours)
        padded_hours = math.hypot(hours, self.buffer_hours)

        disc_cost = self.discrepancy_weight * discrepancy / self.max_discrepancy

        speed = dist / padded_hours
        speed_cost = self.speed_weight * speed  / self.max_average_knots

        time_cost = self.time_metric_weight * (hours / self.time_metric_scale_hours)

        return ( 
                 disc_cost +
                 speed_cost +
                 time_cost +
                 overlap_cost
                 )



    def update_hypotheses(self, hypotheses, segment):
        updated_hypotheses = []
        for h in hypotheses:
            date = segment.last_msg_of_day.timestamp.date()
            for i, track in enumerate(h['tracks']):
                if not track.is_active:
                    continue
                track_list = list(h['tracks'])
                # Use last msg per day in both cases so we get decay when it's back to back
                # segs. We don't use first because we don't always have the first message available
                last_msg = track.last_msg
                days_since_track = (segment.last_msg_of_day.timestamp - 
                                    last_msg.timestamp).total_seconds() / S_PER_DAY
                if segment.last_msg_of_day.timestamp > last_msg.timestamp:
                    last_msg = segment.last_msg_of_day

                decay =  self.decay_per_day ** days_since_track

                new_sig_dict = {}
                for j, sigkey in enumerate(Signature._fields):
                    sigcomp = track.signature[j].copy()
                    for k in sigcomp:
                        sigcomp[k] *= decay
                    for k, v in getattr(segment, sigkey):
                        sigcomp[k] = sigcomp.get(k, 0) + v 
                    new_sig_dict[sigkey] = sigcomp
                new_sig = Signature(**new_sig_dict)

                track_list[i] = track._replace(
                         seg_ids=tuple(track.seg_ids) + (segment.aug_id,),
                         count=track.count + segment.daily_msg_count,
                         decayed_count=decay * track.decayed_count + segment.daily_msg_count,
                         signature=new_sig,
                         last_msg=last_msg,
                         parent_track=track,
                    )

                updated_hypotheses.append({'cost' : h['cost'] + self.find_cost(track, segment), 
                                           'tracks' : tuple(track_list)})
            track_list = list(h['tracks'])
            track_list.append(Track(id=segment.aug_id, 
                                  seg_ids=(segment.aug_id,), 
                                  count=segment.daily_msg_count,
                                  decayed_count=segment.daily_msg_count,
                                  is_active=True, 
                                  signature=self.get_seg_sig(segment),
                                  last_msg=segment.last_msg_of_day,
                                  parent_track=None,
                            ))
            updated_hypotheses.append({'cost' : h['cost'] + self.base_track_cost, 
                                      'tracks' :  self.prune_tracks(track_list)})
        return updated_hypotheses  

    _seg_joining_costs = {}

    def find_cost(self, track, seg):
        key = (track.last_msg, seg.aug_id)
        if key not in self._seg_joining_costs:
            self._seg_joining_costs[key] = self.compute_cost(track.last_msg, seg)
        return  self._seg_joining_costs[key] + self.signature_cost(track, seg)

    def prune_hypotheses(self, hypotheses_list, n):
        def count_cost(h):
            return (self.count_weight * sum(x.count ** 0.5 for x in h['tracks']) /
                   (self.base_count + sum(x.count for x in h['tracks']))**0.5)
        hypotheses_list = sorted(hypotheses_list, key=lambda x: x['cost']+ count_cost(x))
        return hypotheses_list[:n]

    def prune_tracks(self, tracks):
        active_tracks = [x for x in tracks if x.is_active]
        if len(active_tracks) > self.max_active_tracks:
            active_tracks.sort(key = lambda x: (x.decayed_count, x.id), reverse=True)
            inactive_tracks = [x for x in tracks if not x.is_active]
            pruned_tracks = inactive_tracks + active_tracks[:self.max_active_tracks]
            for track in active_tracks[self.max_active_tracks:]:
                pruned_tracks.append(track._replace(is_active=False))
            pruned_tracks = tuple(pruned_tracks)
        else:
            pruned_tracks = tuple(tracks)
        return pruned_tracks


    def create_tracks(self, start_date, tracks, segs, look_ahead=1):
        segs = self.filter_and_sort(segs, self.min_seg_size, start_date)

        hypotheses = [{'cost' : 0, 'tracks' : self.prune_tracks(tracks)}]

        for i, seg in enumerate(segs):
            if not self.seg_duration(seg).total_seconds() > 0:
                continue
            hypotheses = self.update_hypotheses(hypotheses, seg)
            hypotheses = self.prune_hypotheses(hypotheses, self.max_hypotheses)
        [final_hypothesis] = self.prune_hypotheses(hypotheses, 1)

        return list(final_hypothesis['tracks'])


