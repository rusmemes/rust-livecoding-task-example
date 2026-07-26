Task: Write a connection pooling library. Three main functions: add a connection to the pool, remove a connection from the pool, and obtain a connection for use with the connection allocation strategy.

Additional requirements:
1. The library must support any connection type.
2. Methods must be asynchronous.
3. The connection selection logic can change at runtime; implement any one of them.
4. If there are no free connections, the call waits for the first one to become available.
5. After completing the work, the connection must be returned to the pool automatically.
6. The library must be thread-safe—a single pool object can be used by multiple threads.
