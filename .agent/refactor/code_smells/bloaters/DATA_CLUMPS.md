# Data Clumps

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Category

Bloaters

## Description

The same group of data items appears together in multiple places-as function parameters, struct fields, or local variables. These related values should be extracted into their own struct or module.

## Signs and Symptoms

- The same 2-3 parameters appear together in multiple function signatures.
- Multiple structs contain the same subset of fields.
- Functions frequently extract the same fields from a map or struct to pass along.
- Related values are passed separately but always used together.
- Changing one value in the group often requires changing others.

## Causes

- Incremental development without recognizing emerging patterns.
- Copy-paste of function signatures.
- Avoiding "premature abstraction" past the point where abstraction is warranted.
- Lack of domain modeling.

## Example

```elixir
defmodule GeoService do
  def distance(lat1, lng1, lat2, lng2) do
    # Calculate distance between two points
    :math.sqrt(:math.pow(lat2 - lat1, 2) + :math.pow(lng2 - lng1, 2))
  end

  def midpoint(lat1, lng1, lat2, lng2) do
    {(lat1 + lat2) / 2, (lng1 + lng2) / 2}
  end

  def format_location(lat, lng) do
    "#{lat}, #{lng}"
  end
end

defmodule Store do
  defstruct [:name, :address, :store_lat, :store_lng]
end

defmodule Warehouse do
  defstruct [:id, :capacity, :warehouse_lat, :warehouse_lng]
end

defmodule DeliveryService do
  def calculate_route(store_lat, store_lng, warehouse_lat, warehouse_lng, customer_lat, customer_lng) do
    # Uses the same lat/lng pairs repeatedly
    leg1 = GeoService.distance(store_lat, store_lng, warehouse_lat, warehouse_lng)
    leg2 = GeoService.distance(warehouse_lat, warehouse_lng, customer_lat, customer_lng)
    leg1 + leg2
  end
end
```

## Refactored

```elixir
defmodule GeoService.Coordinate do
  @enforce_keys [:lat, :lng]
  defstruct [:lat, :lng]

  @type t :: %__MODULE__{lat: float(), lng: float()}

  def new(lat, lng) when is_number(lat) and is_number(lng) do
    %__MODULE__{lat: lat, lng: lng}
  end
end

defmodule GeoService do
  alias GeoService.Coordinate

  @spec distance(Coordinate.t(), Coordinate.t()) :: float()
  def distance(%Coordinate{} = a, %Coordinate{} = b) do
    :math.sqrt(:math.pow(b.lat - a.lat, 2) + :math.pow(b.lng - a.lng, 2))
  end

  @spec midpoint(Coordinate.t(), Coordinate.t()) :: Coordinate.t()
  def midpoint(%Coordinate{} = a, %Coordinate{} = b) do
    Coordinate.new((a.lat + b.lat) / 2, (a.lng + b.lng) / 2)
  end

  @spec format(Coordinate.t()) :: String.t()
  def format(%Coordinate{lat: lat, lng: lng}) do
    "#{lat}, #{lng}"
  end
end

defmodule Store do
  alias GeoService.Coordinate

  defstruct [:name, :address, :location]

  @type t :: %__MODULE__{
    name: String.t(),
    address: String.t(),
    location: Coordinate.t()
  }
end

defmodule Warehouse do
  alias GeoService.Coordinate

  defstruct [:id, :capacity, :location]

  @type t :: %__MODULE__{
    id: String.t(),
    capacity: integer(),
    location: Coordinate.t()
  }
end

defmodule DeliveryService do
  alias GeoService.Coordinate

  @spec calculate_route(Coordinate.t(), Coordinate.t(), Coordinate.t()) :: float()
  def calculate_route(store_location, warehouse_location, customer_location) do
    leg1 = GeoService.distance(store_location, warehouse_location)
    leg2 = GeoService.distance(warehouse_location, customer_location)
    leg1 + leg2
  end
end
```

## Treatment

- Group the repeated data into a struct when it regularly travels together.
- **Introduce Parameter Struct**: Replace multiple parameters with a single struct.
- **Preserve Whole Struct**: Pass the struct instead of extracting fields.
- **Move Function**: If functions primarily operate on the clump, move them to the new module.

## Why Refactor

- Reduces parameter count across many functions.
- Makes the relationship between data explicit.
- Centralizes validation and construction logic.
- Improves code readability with meaningful type names.
- Changes to the data structure happen in one place.

## How to Identify

Look for these patterns:

1. **Parameter pairs/triples**: `(x, y)`, `(lat, lng)`, `(start_date, end_date)`
2. **Prefixed field names**: `user_name, user_email, user_id` -> `User.t()`
3. **Repeated extractions**: `%{lat: lat, lng: lng} = location` appearing multiple times

## Related Smells

- `Primitive Obsession`
- `Long Parameter List`
- `Duplicate Code`

## Related Refactoring Techniques

- `Extract Module`
- `Introduce Parameter Struct`
- `Preserve Whole Struct`
- `Move Function`
