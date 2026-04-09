namespace SignalTracker.DTO.PythonBridge
{
    public class DriveTestRowsRequest
    {
        public List<long> SessionIds { get; set; } = new();
        public bool IncludeNeighbour { get; set; } = true;
        public int Limit { get; set; } = 50000;
        public int Offset { get; set; } = 0;
    }
}
