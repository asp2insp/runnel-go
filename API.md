# API

# Streams

## mergeInto(stream)
## fork()
## split()
## limit??
## close()
## drain()
## insert(message)

## consume(handler)


Not sure if these are filters or stream apis. I'm leaning towards filters.
## onMessage
## onOpen
## onClose
## onChange
## onInsert
## onDelete
## onAny


# Filters

## builder.[filter]+.build
## from
## until
## property
## authorType
## before
## after
## around
## sample
## dedup

Each returns a filtered stream. filtered streams are lazily built so each message is only queried once per filtered view. Building a filter on a stream does not change the underlying stream.

when a filtered view is created, it runs through all available historical data, then continues to update whenever new data is available.

Filters are always composable and will warn when you try to compose contradictory filters.

If the PubSideFilters setting is enabled, filters will eventually be pushed over connections to reduce the number of messages sent.

Filtered views are immutable once built.

# Modifiers
Modifiers edit messages in flight, one at a time.

## addTag
## removeTag
## anonymize
## clearMetadata
## clearPayload
## editPayload

like aggregators, modifiers take a stream and produce a new stream.

# Aggregators
An aggregator processes a stream of events and collapses them into a read-only data structure. Each aggregator has its own stream that allows filtering, subscription, and yes, aggregation so that you can express complex transformations from simple building blocks.

## count
## sum
## group
## unique
## average
## stdev
## max
## min
## snapshot
