"""
Example of user based mapper and reducer for MR job.
Both functions accept context which carries on the record value,
i.e. the record is accessed via ctx.value. Also context can
emit a value via emit call.
"""

def mapper(ctx):
    "Read given context and yield key (job-id) and values (task)"
    rec = ctx.value
    jid = rec['jobid']
    if jid is not None:
        ctx.emit(jid, rec['fwjr']['task'])

def reducer(ctx):
    "Emit empty key and some data structure via given context"
    ctx.emit('', {'jobid': ctx.key, 'task': ctx.values})
