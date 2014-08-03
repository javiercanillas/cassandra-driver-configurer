cassandra-driver-configurer
===========================

Cassandra driver configurer based on TypeSafe configuration given.

For example:
...
contactPoints: [
    "127.0.0.1"
    ],
compression: NONE,
protocolVersion: 2,
socketOptions: {
    connectTimeoutMillis: 1000,
    readTimeoutMillis: 5000
},
retryPolicy: {
    type: LoggingRetryPolicy,
    configuration: {
        type: DefaultRetryPolicy
    }
},
reconnectionPolicy: {
    type: ExponentialReconnectionPolicy,
    configuration: {
        baseDelayMs: 5000,
        maxDelayMs: 60000
    }
}
...

It might have some missing attributes or configuration entries.
These are the ones I used on my internal applications, but be my guest and make me a merge request to add the missing ones.

Cheers!!!

Javier Canillas
