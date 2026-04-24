using System.Data;
using Microsoft.EntityFrameworkCore;
using SignalTracker.DTO.PythonBridge;
using SignalTracker.Helper;
using SignalTracker.Models;

namespace SignalTracker.Services
{
    public class PythonBridgeService
    {
        private const int DefaultBatchSize = 2000;

        private readonly ApplicationDbContext _db;
        private readonly IConfiguration _configuration;

        public PythonBridgeService(ApplicationDbContext db, IConfiguration configuration)
        {
            _db = db;
            _configuration = configuration;
        }

        public bool IsAuthorized(string? incomingKey)
        {
            var configuredKey =
                _configuration["PythonBridge:ApiKey"]
                ?? Environment.GetEnvironmentVariable("PYTHON_BRIDGE_API_KEY");

            if (string.IsNullOrWhiteSpace(configuredKey))
            {
                return true;
            }

            if (string.IsNullOrWhiteSpace(incomingKey))
            {
                return false;
            }

            return string.Equals(
                configuredKey.Trim(),
                incomingKey.Trim(),
                StringComparison.Ordinal
            );
        }

        public async Task<(int Limit, int Offset, List<Dictionary<string, object?>> Rows)> GetDriveTestRowsAsync(
            DriveTestRowsRequest request,
            CancellationToken cancellationToken = default
        )
        {
            var sessionIds = request.SessionIds
                .Where(id => id > 0)
                .Distinct()
                .ToList();

            if (sessionIds.Count == 0)
            {
                throw new ArgumentException("No valid SessionIds provided.");
            }

            var limit = Math.Clamp(request.Limit, 1, 50000);
            var offset = Math.Max(request.Offset, 0);

            var servingSql = @"
                SELECT
                    lat, lon, rsrp, nodeb_id, band, network, pci, earfcn, session_id
                FROM tbl_network_log
                WHERE session_id IN ({0})";

            var neighbourSql = @"
                SELECT
                    lat, lon, rsrp, nodeb_id,
                    NULL AS band,
                    NULL AS network,
                    NULL AS pci,
                    NULL AS earfcn,
                    session_id
                FROM tbl_network_log_neighbour
                WHERE session_id IN ({0})";

            var conn = _db.Database.GetDbConnection();
            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync(cancellationToken);
            }

            await using var command = conn.CreateCommand();
            var inClause = PythonBridgeDbTool.BuildInClause(command, sessionIds, "sid");
            var servingQuery = string.Format(servingSql, inClause);
            var neighbourQuery = string.Format(neighbourSql, inClause);

            command.CommandText = request.IncludeNeighbour
                ? $"{servingQuery} UNION ALL {neighbourQuery} LIMIT @lim OFFSET @off;"
                : $"{servingQuery} LIMIT @lim OFFSET @off;";

            PythonBridgeDbTool.AddParam(command, "@lim", limit);
            PythonBridgeDbTool.AddParam(command, "@off", offset);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var rows = await PythonBridgeDbTool.ReadRowsAsync(reader, cancellationToken);

            return (limit, offset, rows);
        }

        public async Task<int> SavePredictionDataAsync(
            PredictionDataBulkRequest request,
            CancellationToken cancellationToken = default
        )
        {
            var rows = request.Rows ?? new List<PredictionDataRow>();
            if (rows.Count == 0)
            {
                return 0;
            }

            if (request.ReplaceProjectData)
            {
                await _db.tbl_prediction_data
                    .Where(x => x.tbl_project_id == (int)request.ProjectId)
                    .ExecuteDeleteAsync(cancellationToken);
            }

            var now = DateTime.UtcNow;
            var previousAutoDetect = _db.ChangeTracker.AutoDetectChangesEnabled;
            _db.ChangeTracker.AutoDetectChangesEnabled = false;

            try
            {
                var inserted = 0;

                foreach (var batch in rows.Chunk(DefaultBatchSize))
                {
                    var entities = batch.Select(r => new tbl_prediction_data
                    {
                        tbl_project_id = (int)request.ProjectId,
                        lat = r.lat.HasValue ? (float?)r.lat.Value : null,
                        lon = r.lon.HasValue ? (float?)r.lon.Value : null,
                        rsrp = r.rsrp.HasValue ? (float?)r.rsrp.Value : null,
                        rsrq = r.rsrq.HasValue ? (float?)r.rsrq.Value : null,
                        sinr = r.sinr.HasValue ? (float?)r.sinr.Value : null,
                        serving_cell = r.serving_cell,
                        band = r.band,
                        earfcn = r.earfcn,
                        pci = r.pci,
                        network = r.network,
                        azimuth = r.azimuth,
                        tx_power = r.tx_power,
                        height = r.height,
                        reference_signal_power = r.reference_signal_power,
                        mtilt = r.mtilt,
                        etilt = r.etilt,
                        timestamp = now
                    }).ToList();

                    await _db.tbl_prediction_data.AddRangeAsync(entities, cancellationToken);
                    await _db.SaveChangesAsync(cancellationToken);
                    inserted += entities.Count;
                    _db.ChangeTracker.Clear();
                }

                return inserted;
            }
            finally
            {
                _db.ChangeTracker.AutoDetectChangesEnabled = previousAutoDetect;
            }
        }

        public async Task<int> SaveLtePredictionResultsAsync(
            LtePredictionBulkRequest request,
            CancellationToken cancellationToken = default
        )
        {
            var rows = request.Rows ?? new List<LtePredictionRow>();
            if (rows.Count == 0)
            {
                return 0;
            }

            var previousAutoDetect = _db.ChangeTracker.AutoDetectChangesEnabled;
            _db.ChangeTracker.AutoDetectChangesEnabled = false;

            try
            {
                var inserted = 0;

                foreach (var batch in rows.Chunk(DefaultBatchSize))
                {
                    var now = DateTime.UtcNow;
                    var entities = batch.Select(r => new tbl_lte_prediction_results
                    {
                        ProjectId = request.ProjectId,
                        JobId = request.JobId ?? string.Empty,
                        Lat = r.lat ?? 0.0,
                        Lon = r.lon ?? 0.0,
                        PredRsrp = r.pred_rsrp,
                        PredRsrq = r.pred_rsrq,
                        PredSinr = r.pred_sinr,
                        SiteId = r.site_id,
                        CreatedAt = now
                    }).ToList();

                    await _db.Tbl_lte_prediction_results.AddRangeAsync(entities, cancellationToken);
                    await _db.SaveChangesAsync(cancellationToken);
                    inserted += entities.Count;
                    _db.ChangeTracker.Clear();
                }

                return inserted;
            }
            finally
            {
                _db.ChangeTracker.AutoDetectChangesEnabled = previousAutoDetect;
            }
        }

        public async Task<int> SaveLtePredictionRefinedAsync(
            LtePredictionRefinedBulkRequest request,
            CancellationToken cancellationToken = default
        )
        {
            var rows = request.Rows ?? new List<LtePredictionRefinedRow>();
            if (rows.Count == 0)
            {
                return 0;
            }

            var previousAutoDetect = _db.ChangeTracker.AutoDetectChangesEnabled;
            _db.ChangeTracker.AutoDetectChangesEnabled = false;

            try
            {
                var inserted = 0;

                foreach (var batch in rows.Chunk(DefaultBatchSize))
                {
                    var now = DateTime.UtcNow;
                    var entities = batch.Select(r => new tbl_lte_prediction_results_refined
                    {
                        project_id = request.ProjectId,
                        job_id = request.JobId ?? string.Empty,
                        lat = r.lat ?? 0.0,
                        lon = r.lon ?? 0.0,
                        site_id = r.site_id,
                        pred_rsrp_top2_avg = r.pred_rsrp_top2_avg,
                        pred_rsrp_top3_avg = r.pred_rsrp_top3_avg,
                        measured_dt_rsrp = r.measured_dt_rsrp,
                        created_at = now
                    }).ToList();

                    await _db.tbl_lte_prediction_results_refined.AddRangeAsync(entities, cancellationToken);
                    await _db.SaveChangesAsync(cancellationToken);
                    inserted += entities.Count;
                    _db.ChangeTracker.Clear();
                }

                return inserted;
            }
            finally
            {
                _db.ChangeTracker.AutoDetectChangesEnabled = previousAutoDetect;
            }
        }

        public async Task<(bool ProjectExists, long SiteNoMlCount)> PredictionDebugSummaryAsync(
            long projectId,
            CancellationToken cancellationToken = default
        )
        {
            var projectExists = await _db.tbl_project
                .AsNoTracking()
                .AnyAsync(p => p.id == projectId, cancellationToken);

            var siteCount = 0L;
            var conn = _db.Database.GetDbConnection();
            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync(cancellationToken);
            }

            await using var command = conn.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM site_noMl WHERE project_id = @pid";
            PythonBridgeDbTool.AddParam(command, "@pid", projectId);

            var scalar = await command.ExecuteScalarAsync(cancellationToken);
            if (scalar != null && scalar != DBNull.Value)
            {
                siteCount = Convert.ToInt64(scalar);
            }

            return (projectExists, siteCount);
        }

        public async Task<tbl_project?> GetProjectAsync(
            long projectId,
            CancellationToken cancellationToken = default
        )
        {
            return await _db.tbl_project
                .AsNoTracking()
                .FirstOrDefaultAsync(p => p.id == projectId, cancellationToken);
        }

        public async Task<List<Dictionary<string, object?>>> GetProjectRegionsAsync(
            long projectId,
            CancellationToken cancellationToken = default
        )
        {
            var conn = _db.Database.GetDbConnection();
            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync(cancellationToken);
            }

            await using var command = conn.CreateCommand();
            command.CommandText = @"
                SELECT
                    id,
                    name,
                    ST_AsText(region) AS region_wkt,
                    area
                FROM map_regions
                WHERE tbl_project_id = @pid
                  AND status = 1
                ORDER BY id;";
            PythonBridgeDbTool.AddParam(command, "@pid", projectId);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await PythonBridgeDbTool.ReadRowsAsync(reader, cancellationToken);
        }

        public async Task<(int Limit, int Offset, List<Dictionary<string, object?>> Rows)> GetReportNetworkLogsAsync(
            SessionIdsPagedRequest request,
            CancellationToken cancellationToken = default
        )
        {
            var sessionIds = request.SessionIds
                .Where(id => id > 0)
                .Distinct()
                .ToList();

            if (sessionIds.Count == 0)
            {
                throw new ArgumentException("No valid SessionIds provided.");
            }

            var limit = Math.Clamp(request.Limit, 1, 50000);
            var offset = Math.Max(request.Offset, 0);

            var conn = _db.Database.GetDbConnection();
            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync(cancellationToken);
            }

            await using var command = conn.CreateCommand();
            var inClause = PythonBridgeDbTool.BuildInClause(command, sessionIds, "sid");
            command.CommandText = $@"
                SELECT *
                FROM tbl_network_log
                WHERE session_id IN ({inClause})
                ORDER BY session_id, timestamp, id
                LIMIT @lim OFFSET @off;";
            PythonBridgeDbTool.AddParam(command, "@lim", limit);
            PythonBridgeDbTool.AddParam(command, "@off", offset);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var rows = await PythonBridgeDbTool.ReadRowsAsync(reader, cancellationToken);
            return (limit, offset, rows);
        }

        public async Task<List<Dictionary<string, object?>>> GetSessionsAsync(
            IReadOnlyList<long> sessionIds,
            CancellationToken cancellationToken = default
        )
        {
            var validIds = sessionIds
                .Where(id => id > 0)
                .Distinct()
                .ToList();

            if (validIds.Count == 0)
            {
                return new List<Dictionary<string, object?>>();
            }

            var conn = _db.Database.GetDbConnection();
            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync(cancellationToken);
            }

            await using var command = conn.CreateCommand();
            var inClause = PythonBridgeDbTool.BuildInClause(command, validIds, "sid");
            command.CommandText = $@"
                SELECT id, start_time, end_time, distance
                FROM tbl_session
                WHERE id IN ({inClause})
                ORDER BY start_time, id;";

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await PythonBridgeDbTool.ReadRowsAsync(reader, cancellationToken);
        }

        public async Task<tbl_user?> GetUserByIdAsync(
            int userId,
            CancellationToken cancellationToken = default
        )
        {
            return await _db.tbl_user
                .AsNoTracking()
                .FirstOrDefaultAsync(u => u.id == userId, cancellationToken);
        }

        public async Task<thresholds?> GetUserThresholdsAsync(
            int userId,
            CancellationToken cancellationToken = default
        )
        {
            var userSetting = await _db.thresholds
                .AsNoTracking()
                .Where(x => x.user_id == userId && x.is_default == 0)
                .OrderByDescending(x => x.id)
                .FirstOrDefaultAsync(cancellationToken);

            if (userSetting != null)
            {
                return userSetting;
            }

            var defaultSetting = await _db.thresholds
                .AsNoTracking()
                .Where(x => x.is_default == 1 && (x.user_id == null || x.user_id == 0))
                .OrderByDescending(x => x.id)
                .FirstOrDefaultAsync(cancellationToken);

            if (defaultSetting != null)
            {
                return defaultSetting;
            }

            return await _db.thresholds
                .AsNoTracking()
                .OrderBy(x => x.id)
                .FirstOrDefaultAsync(cancellationToken);
        }

        public async Task<bool> UpdateProjectDownloadPathAsync(
            long projectId,
            string downloadPath,
            CancellationToken cancellationToken = default
        )
        {
            var project = await _db.tbl_project.FirstOrDefaultAsync(
                p => p.id == projectId,
                cancellationToken
            );

            if (project == null)
            {
                return false;
            }

            project.Download_path = downloadPath;
            await _db.SaveChangesAsync(cancellationToken);
            return true;
        }
    }
}
