
Bug fix needed in impulse_reporting.

Error: AttributeError: 'NoneType' object has no attribute 'filter_container_tags' in impulse_reporting/events/group_by_time_event.py:246 inside GroupByTimeEvent.determine_events.

Root cause: dispatch_events in impulse_reporting/core/report_utils.py only passes solver to ContainerEvent subclasses. GroupByTimeEvent extends Event directly, so it hits the else branch and is called without solver (which defaults to None). But GroupByTimeEvent.determine_events immediately calls solver.filter_container_tags(spark, query).

Fix — two options, pick one:

Option A (preferred): In impulse_reporting/events/group_by_time_event.py, change the base class:

# Before

class GroupByTimeEvent(Event):

# After

class GroupByTimeEvent(ContainerEvent):
Option B: In impulse_reporting/core/report_utils.py, update the dispatch_events routing condition:

# Before

if issubclass(cls, container_event_cls):

# After

if issubclass(cls, container_event_cls) or issubclass(cls, GroupByTimeEvent):
(and import GroupByTimeEvent at the top of report_utils.py)

After the fix, bump the wheel version, rebuild databricks_impulse, and redeploy.

Option A is cleaner architecturally since GroupByTimeEvent already uses the exact same solver pipeline (filter_container_tags → filter_container_metrics) as ContainerEvent. Just make sure ContainerEvent's __init__ is compatible — if it requires arguments GroupByTimeEvent doesn't need, Option B is the safer path.

---

#COMMENTS:

So regarding option A, GroupByTimeEvent is not a Container Event actually,  so I dont want to inherit it. Why does it throw the error really?



the stack trace:

243 group_by_ms = event._group_by_ms
    245# Resolve containers via solver filter pipeline
**--> 246** container_tags_df = solver.filter_container_tags(spark, query)
    247 container_metrics_df = solver.filter_container_metrics(
    248     spark, query, container_tags_df, pre_filtered_containers_df
    249 )
    251# Rename and cast timest
