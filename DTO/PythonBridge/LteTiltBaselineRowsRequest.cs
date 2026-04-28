namespace SignalTracker.DTO.PythonBridge
{
    public class LteTiltBaselineRowsRequest
    {
        public long ProjectId { get; set; }
        public string? Operator { get; set; }
        public int Limit { get; set; } = 5000;
        public int Offset { get; set; } = 0;
    }
}
