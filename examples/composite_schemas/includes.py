from sparkorm import Struct, Timestamp, String


#
# Simple example of re-using common fields via `includes`

class EventMetadata(Struct):
    correlation_id = String(nullable=False)
    event_time = Timestamp(nullable=False)


class RegistrationEvent(Struct):
    class Meta:
        includes = [EventMetadata]
    user_id = String(nullable=False)


#
# Here's what the schema looks like

prettified_registration_event_schema = """
StructType([
    StructField('user_id', StringType(), False), 
    StructField('correlation_id', StringType(), False), 
    StructField('event_time', TimestampType(), False)])
"""
