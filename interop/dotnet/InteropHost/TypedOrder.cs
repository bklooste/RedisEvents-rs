using System.Text.Json.Serialization;

namespace Interop;

// The typed payload of gate G4. The default System.Text.Json shape on purpose: what a C# service
// gets with no attributes is what a Rust `#[serde(rename_all = "PascalCase")]` struct must read.
public sealed class TypedOrder
{
    public string OrderId { get; set; } = "";
    public long Quantity { get; set; }
    public decimal Price { get; set; }
    public OrderStatus Status { get; set; }
    public DateTimeOffset PlacedAt { get; set; }
    public List<string> Tags { get; set; } = [];
    public TypedCustomer Customer { get; set; } = new();
    public string? Note { get; set; }
}

public sealed class TypedCustomer
{
    public string Name { get; set; } = "";
}

public enum OrderStatus
{
    Pending = 0,
    Placed = 1,
    Settled = 2,
}

[JsonSerializable(typeof(TypedOrder))]
public sealed partial class TypedOrderJson : JsonSerializerContext;
