using System.ComponentModel.DataAnnotations;

namespace USPSTracker.Models
{
    public class TrackingDetail
    {
        [Required]
        public required string Date { get; set; }
        
        [Required]
        public required string Time { get; set; }
        
        [Required]
        public required string Status { get; set; }
        
        [Required]
        public required string Location { get; set; }
    }
}