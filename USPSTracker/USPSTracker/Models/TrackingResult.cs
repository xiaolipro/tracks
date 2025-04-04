using System.ComponentModel.DataAnnotations;

namespace USPSTracker.Models
{
    public class TrackingResult
    {
        [Required]
        public required string TrackingNumber { get; set; }
        
        [Required]
        public required string Status { get; set; }
        
        public List<TrackingDetail> Details { get; set; } = new();
        
        public string? Error { get; set; }
    }
}