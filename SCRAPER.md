## Slicing

### Strategies

Take into account errors, request time, doc suggestion

#### Max Range (pagination + ordered)

Search like.
`from` should be at the minimum, `to` to today.
Result is ordered/paginated with latest result updated first.
It's similar to a search method

- Largest interval possible

#### Unordered (pagination + no order)

The response have pagination but it's not ordered so we may want to have smaller interval

- Stock => maximum
- Flux => from last time only

#### Result Limit (no pagination)

The response can't contain "X" number of result and so the pagination should reflect that.

- Large interval to small
