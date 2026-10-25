//! The position flusher. **Owner: C3** (port of `PositionFlusher.cs`): batches positions, cross-
//! checks foreign writers against the ownership hash, applies reset markers through the hand-off
//! agreed with C2, and publishes the minimum position for the trimmer (C5). Crate-private.
