### 2

    [id: 1]
    [published: true]
    [id: 1, published: true]
    [published_at: nil]
    [published: [true, false]]
    [title: "hello"]

|-----------------|-----------------|
|        1        |        2        |
|-----------------|-----------------|
| `:id`           | `1`             |
| `:published`    | `true`          |
| `:published_at` | `nil`           |
| `:published`    | `[true, false]` |
| `:title`        | `"hello"`       |

### 3

    [and: [[id: 1]]]
    [and: [[id: 1], [published: true]]]
    [and: [[id: 1, title: "hello"], [published: true]]]

|-----------------|----------------|------------|
|        1        |        2       |    3       |
|-----------------|----------------|------------|
|      `:and`     |     `:id`      |    `1`     |
|      `:and`     |  `:published`  |   `true`   |
|      `:and`     |    `:title`    |  `"hello"` |

### 4

    [not: [published: [in: [true]]]]
    [not: [published: [==: [true]]]]
    [not: [published: [!=: [true, false]]]]

    [not: [views: [>: 10]]]
    [not: [views: [>=: 10]]]
    [not: [views: [<: 10]]]
    [not: [views: [<=: 10]]]
    [not: [views: [==: 10]]]
    [not: [views: [!=: 10]]]
    [not: [views: [gt: 10]]]
    [not: [views: [gte: 10]]]
    [not: [views: [lt: 10]]]
    [not: [views: [lte: 10]]]

    [or: [[id: 1, or: [title: "hello", body: "world"]], [published: true]]]

|--------------|--------------|----------|-----------------|
|   1          |     2        |    3     |        4        |
|--------------|--------------|----------|-----------------|
| `:not`       | `:published` | `:in`    | `[true]`        |
| `:not`       | `:published` | `:==`    | `[true]`        |
| `:not`       | `:published` | `:!=`    | `[true, false]` |
|--------------|--------------|----------|-----------------|
| `:not`       | `:views`     | `:>`     | `10`            |
| `:not`       | `:views`     | `:>=`    | `10`            |
| `:not`       | `:views`     | `:<`     | `10`            |
| `:not`       | `:views`     | `:<=`    | `10`            |
| `:not`       | `:views`     | `:==`    | `10`            |
| `:not`       | `:views`     | `:!=`    | `10`            |
| `:not`       | `:views`     | `:gt`    | `10`            |
| `:not`       | `:views`     | `:gte`   | `10`            |
| `:not`       | `:views`     | `:lt`    | `10`            |
| `:not`       | `:views`     | `:lte`   | `10`            |
|--------------|--------------|----------|-----------------|
| `:or`        | `:id`        | 1        |                 |
| `:or`        | `:or`        | `:title` | `"hello"`       |
| `:or`        | `:or`        | `:title` | `"body"`        |
| `:published` | `true`       |          |                 |