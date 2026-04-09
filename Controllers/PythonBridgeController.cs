using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SignalTracker.DTO.PythonBridge;
using SignalTracker.Services;

namespace SignalTracker.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class PythonBridgeController : ControllerBase
    {
        private readonly PythonBridgeService _pythonBridgeService;

        public PythonBridgeController(PythonBridgeService pythonBridgeService)
        {
            _pythonBridgeService = pythonBridgeService;
        }

        private bool IsAuthorized()
        {
            Request.Headers.TryGetValue("X-Python-Bridge-Key", out var incoming);
            return _pythonBridgeService.IsAuthorized(incoming.ToString());
        }

        private IActionResult? EnsureAuthorized()
        {
            if (IsAuthorized())
            {
                return null;
            }
            return Unauthorized(new { Status = 0, Message = "Invalid bridge key." });
        }

        [AllowAnonymous]
        [HttpPost("GetDriveTestRows")]
        public async Task<IActionResult> GetDriveTestRows([FromBody] DriveTestRowsRequest request)
        {
            var authResult = EnsureAuthorized();
            if (authResult is not null) return authResult;

            if (request == null || request.SessionIds == null || request.SessionIds.Count == 0)
            {
                return BadRequest(new { Status = 0, Message = "SessionIds are required." });
            }

            var sessionIds = request.SessionIds
                .Where(id => id > 0)
                .Distinct()
                .ToList();
            if (sessionIds.Count == 0)
            {
                return BadRequest(new { Status = 0, Message = "No valid SessionIds provided." });
            }

            request.SessionIds = sessionIds;

            var result = await _pythonBridgeService.GetDriveTestRowsAsync(
                request,
                HttpContext.RequestAborted
            );

            return Ok(new
            {
                Status = 1,
                Count = result.Rows.Count,
                Limit = result.Limit,
                Offset = result.Offset,
                Data = result.Rows
            });
        }

        [AllowAnonymous]
        [HttpPost("SavePredictionData")]
        public async Task<IActionResult> SavePredictionData([FromBody] PredictionDataBulkRequest request)
        {
            var authResult = EnsureAuthorized();
            if (authResult is not null) return authResult;

            if (request == null || request.ProjectId <= 0)
            {
                return BadRequest(new { Status = 0, Message = "ProjectId is required." });
            }

            if (request.ProjectId > int.MaxValue)
            {
                return BadRequest(new { Status = 0, Message = "ProjectId is out of supported range." });
            }

            var rows = request.Rows ?? new List<PredictionDataRow>();
            if (rows.Count == 0)
            {
                return Ok(new { Status = 1, Inserted = 0 });
            }

            request.Rows = rows;
            var inserted = await _pythonBridgeService.SavePredictionDataAsync(
                request,
                HttpContext.RequestAborted
            );

            return Ok(new { Status = 1, Inserted = inserted });
        }

        [AllowAnonymous]
        [HttpPost("SaveLtePredictionResults")]
        public async Task<IActionResult> SaveLtePredictionResults([FromBody] LtePredictionBulkRequest request)
        {
            var authResult = EnsureAuthorized();
            if (authResult is not null) return authResult;

            if (request == null || request.ProjectId <= 0)
            {
                return BadRequest(new { Status = 0, Message = "ProjectId is required." });
            }

            if (request.Rows == null || request.Rows.Count == 0)
            {
                return Ok(new { Status = 1, Inserted = 0 });
            }

            var inserted = await _pythonBridgeService.SaveLtePredictionResultsAsync(
                request,
                HttpContext.RequestAborted
            );

            return Ok(new { Status = 1, Inserted = inserted });
        }

        [AllowAnonymous]
        [HttpPost("SaveLtePredictionRefined")]
        public async Task<IActionResult> SaveLtePredictionRefined([FromBody] LtePredictionRefinedBulkRequest request)
        {
            var authResult = EnsureAuthorized();
            if (authResult is not null) return authResult;

            if (request == null || request.ProjectId <= 0)
            {
                return BadRequest(new { Status = 0, Message = "ProjectId is required." });
            }

            if (request.Rows == null || request.Rows.Count == 0)
            {
                return Ok(new { Status = 1, Inserted = 0 });
            }

            var inserted = await _pythonBridgeService.SaveLtePredictionRefinedAsync(
                request,
                HttpContext.RequestAborted
            );

            return Ok(new { Status = 1, Inserted = inserted });
        }

        [AllowAnonymous]
        [HttpGet("PredictionDebugSummary")]
        public async Task<IActionResult> PredictionDebugSummary([FromQuery] long projectId)
        {
            var authResult = EnsureAuthorized();
            if (authResult is not null) return authResult;

            if (projectId <= 0)
            {
                return BadRequest(new { Status = 0, Message = "projectId is required" });
            }

            var summary = await _pythonBridgeService.PredictionDebugSummaryAsync(
                projectId,
                HttpContext.RequestAborted
            );

            return Ok(new
            {
                Status = 1,
                project_exists = summary.ProjectExists,
                site_noMl_count = summary.SiteNoMlCount,
                source = "signal-trackers"
            });
        }
    }
}
