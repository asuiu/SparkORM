from sparkorm import Struct, Timestamp, String


class LogEntryMetadata(Struct):
    logged_at = Timestamp(nullable=False)


class PageViewLogEntry(Struct):
    class Meta:
        implements = [LogEntryMetadata]
    page_id = String(nullable=False)

# the above class declaration will fail with the following StructImplementationError error:
#   Struct 'RegistrationEvent' does not implement field 'correlation_id' required by struct 'EventMetadata'
