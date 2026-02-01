[![Build Status][ci-badge]][ci-url]

# Vigil

An event query processing engine. Vigil provides a database for storing, indexing, and querying events using [EventQL](https://github.com/YoEight/eventql-parser), a custom query language designed for event-driven applications.

## Features

- **Event Storage**: Event database with CloudEvents-compatible structure
- **Multiple Indexing Strategies**: Fast lookups by event type and hierarchical subject paths
- **EventQL**: Rich query language with filtering, projection, and aggregation
- **Type-Safe Execution**: Static analysis during parsing with runtime type validation
- **Lazy Evaluation**: Iterator-based query execution for memory efficiency

## Query Language

Vigil uses EventQL for querying events. The language supports:

### Data Sources

```eql
FROM e IN events            -- All events
FROM e IN "companies/acme"  -- Events under a subject path
```

### Filtering and Projection

```eql
FROM e IN events
WHERE e.type == "user-created"
PROJECT INTO {
    id: e.id,
    username: e.data.username
}
```

### Aggregations

```eql
FROM e IN events
GROUP BY e.data.department
PROJECT INTO {
    department: UNIQUE(e.data.department),
    count: COUNT(),
    avgSalary: AVG(e.data.salary)
}

## License

Apache 2.0

[ci-badge]: https://github.com/YoEight/Vigil/workflows/CI/badge.svg
[ci-url]: https://github.com/YoEight/Vigil/actions
