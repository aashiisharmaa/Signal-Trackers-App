using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.Extensions.Configuration;

using SignalTracker.Models; 
using SignalTracker.Services;

namespace SignalTracker.Controllers
{
    [ApiController]
    [Route("api/auth")]
    public class AuthController : ControllerBase
    {
        private const string LegacyGlobalLoginLockKey = "auth:global-login-lock";
        private const string UserLoginLockKeyPrefix = "auth:login-lock:user:";
        private const int UserLoginLockTtlSeconds = 18000;

        private readonly ApplicationDbContext _db;
        private readonly ILogger<AuthController> _logger;
        private readonly IConfiguration _configuration;
        private readonly LicenseFeatureService _licenseFeatureService;
        private readonly RedisService _redis;

        public AuthController(
            ApplicationDbContext db,
            ILogger<AuthController> logger,
            IConfiguration configuration,
            LicenseFeatureService licenseFeatureService,
            RedisService redis)
        {
            _db = db;
            _logger = logger;
            _configuration = configuration;
            _licenseFeatureService = licenseFeatureService;
            _redis = redis;
        }

        private sealed class LoginUserDto
        {
            public int id { get; set; }
            public string email { get; set; } = string.Empty;
            public string? name { get; set; }
            public int m_user_type_id { get; set; }
            public string? country_code { get; set; }
            public int? company_id { get; set; }
        }

        private async Task<LoginUserDto?> FindTwUserAsync(string email, string password)
        {
            var twConnectionString = MySqlConnectionStringHelper.EnsureZeroDateTimeHandling(_configuration.GetConnectionString("MySqlConnection2"));
            if (string.IsNullOrWhiteSpace(twConnectionString))
                return null;

            var optionsBuilder = new DbContextOptionsBuilder<ApplicationDbContext>();
            optionsBuilder.UseMySql(twConnectionString, new MySqlServerVersion(new Version(8, 0, 29)));

            using var twDb = new ApplicationDbContext(optionsBuilder.Options);

            return await twDb.tbl_user
                .AsNoTracking()
                .Where(u => u.email == email && u.password == password && u.isactive == 1)
                .Select(u => new LoginUserDto
                {
                    id = u.id,
                    email = u.email,
                    name = u.name,
                    m_user_type_id = u.m_user_type_id,
                    country_code = u.country_code,
                    company_id = u.company_id
                })
                .FirstOrDefaultAsync();
        }

        [HttpPost("login")]
        public async Task<IActionResult> Login([FromBody] LoginRequest model)
        {
            var requestedCountry = (model.country_code ?? string.Empty).Trim().ToUpperInvariant();
            bool preferTw = requestedCountry == "TW";

            LoginUserDto? user = null;
            var loginSource = "IN";

            if (preferTw)
            {
                user = await FindTwUserAsync(model.Email, model.Password);
                if (user != null) loginSource = "TW";
            }
            else
            {
                // Default authentication on main DB; fallback to TW
                user = await _db.tbl_user
                    .AsNoTracking()
                    .Where(u => u.email == model.Email && u.password == model.Password && u.isactive == 1)
                    .Select(u => new LoginUserDto
                    {
                        id = u.id,
                        email = u.email,
                        name = u.name,
                        m_user_type_id = u.m_user_type_id,
                        country_code = u.country_code,
                        company_id = u.company_id
                    })
                    .FirstOrDefaultAsync();

                if (user == null)
                {
                    user = await FindTwUserAsync(model.Email, model.Password);
                    if (user != null) loginSource = "TW";
                }
            }

            if (user == null)
            {
                return Unauthorized(new { message = "Invalid email or password" });
            }

            if (_redis?.IsConnected != true)
            {
                return StatusCode(503, new { message = "Login service is temporarily unavailable. Please try again." });
            }

            var lockValue = $"{user.id}:{user.email}:{DateTimeOffset.UtcNow:O}";
            var userLockKey = BuildUserLoginLockKey(user.id);
            if (model.ForceLogin == true)
            {
                await _redis.DeleteAsync(userLockKey);
                // Backward compatibility: clear old single global lock key as well.
                await _redis.DeleteAsync(LegacyGlobalLoginLockKey);
            }

            var lockAcquired = await _redis.TrySetStringAsync(userLockKey, lockValue, UserLoginLockTtlSeconds);
            if (!lockAcquired)
            {
                return Unauthorized(new { message = "Sorry, someone is already logged in. Please try again later." });
            }

            var resolvedCountryCode = (string.IsNullOrWhiteSpace(user.country_code) ? loginSource : user.country_code).Trim().ToUpperInvariant();
            var loginCompleted = false;

            // 2. Create claims. CRITICAL: Include 'country_code' so the provider knows which DB to use next.
            try
            {
                var enabledFeatures = await _licenseFeatureService.GetEnabledFeaturesForUserAsync(user.id);

                var claims = new List<Claim>
                {
                    new Claim(ClaimTypes.Email, user.email),
                    new Claim(ClaimTypes.Name, user.name ?? ""),
                    new Claim("country_code", resolvedCountryCode), // This drives the dynamic switch
                    new Claim("m_user_type_id", user.m_user_type_id.ToString()),
                    new Claim("UserId", user.id.ToString()),
                    new Claim("UserTypeId", user.m_user_type_id.ToString()),
                    new Claim("CompanyId", user.company_id?.ToString() ?? "0"),
                    new Claim("company_id", user.company_id?.ToString() ?? "0")
                };

                var claimsIdentity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);

                // 3. Sign in the user with the claims
                await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
                HttpContext.Session.Clear();

                await HttpContext.SignInAsync(
                    CookieAuthenticationDefaults.AuthenticationScheme,
                    new ClaimsPrincipal(claimsIdentity),
                    new AuthenticationProperties { IsPersistent = true });

                HttpContext.Session.SetString("country_code", resolvedCountryCode);
                HttpContext.Session.SetString("UserName", user.email ?? string.Empty);
                HttpContext.Session.SetInt32("UserID", user.id);
                HttpContext.Session.SetInt32("UserType", user.m_user_type_id);
                HttpContext.Session.SetInt32("CompanyId", user.company_id ?? 0);
                loginCompleted = true;

                return Ok(new
                {
                    message = "Login successful",
                    country = resolvedCountryCode,
                    source_db = loginSource,
                    user = new
                    {
                        user.id,
                        user.email,
                        user.name,
                        user.m_user_type_id,
                        user.company_id,
                        user.country_code,
                        enabled_features = enabledFeatures
                    }
                });
            }
            catch (Exception ex)
            {
                if (!loginCompleted)
                {
                    try
                    {
                        await _redis.DeleteAsync(userLockKey);
                    }
                    catch { }
                }

                _logger.LogError(ex, "Error during login for {Email}", model.Email);
                return StatusCode(500, new { message = "An error occurred. Please try again." });
            }
        }

        [HttpPost("logout")]
        public async Task<IActionResult> Logout()
        {
            try
            {
                if (_redis?.IsConnected == true)
                {
                    var claimUserId = User?.FindFirst("UserId")?.Value;
                    var sessionUserId = HttpContext?.Session.GetInt32("UserID")?.ToString();
                    var userIdValue = !string.IsNullOrWhiteSpace(claimUserId) ? claimUserId : sessionUserId;

                    if (int.TryParse(userIdValue, out var parsedUserId) && parsedUserId > 0)
                    {
                        await _redis.DeleteAsync(BuildUserLoginLockKey(parsedUserId));
                    }

                    // Backward compatibility: clear old single global lock key.
                    await _redis.DeleteAsync(LegacyGlobalLoginLockKey);
                }

                await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
                HttpContext.Session.Clear();

                return Ok(new { success = true, message = "Logged out successfully." });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during logout");
                return StatusCode(500, new { success = false, message = "An error occurred. Please try again." });
            }
        }

        private static string BuildUserLoginLockKey(int userId)
            => $"{UserLoginLockKeyPrefix}{userId}";

        [Authorize]
        [HttpGet("status")]
        public async Task<ActionResult<AuthStatusResponse>> GetAuthStatus(CancellationToken ct)
        {
            var email = GetEmailFromClaims(User);
            if (string.IsNullOrWhiteSpace(email)) return Unauthorized();

            // This query will now automatically run against the user's specific database
            var user = await _db.tbl_user
                .AsNoTracking()
                .Where(u => u.email == email)
                .Select(u => new UserSummaryDto
                {
                    id = u.id,
                    name = u.name,
                    email = u.email,
                    m_user_type_id = u.m_user_type_id,
                    country_code = u.country_code, // Added to DTO
                    company_id = u.company_id
                })
                .FirstOrDefaultAsync(ct);

            if (user is null) return NotFound();

            user.enabled_features = await _licenseFeatureService.GetEnabledFeaturesForUserAsync(user.id, ct);

            return Ok(new AuthStatusResponse { user = user });
        }

        private static string? GetEmailFromClaims(ClaimsPrincipal user)
        {
            var candidates = new[] { ClaimTypes.Email, "email", ClaimTypes.Name };
            foreach (var type in candidates)
            {
                var value = user.FindFirst(type)?.Value;
                if (!string.IsNullOrWhiteSpace(value) && value.Contains('@')) return value;
            }
            return null;
        }
    }

    public class LoginRequest 
    {
        public string Email { get; set; } = default!;
        public string Password { get; set; } = default!;
        public string? country_code { get; set; }
        public bool? ForceLogin { get; set; }
    }

    public sealed class AuthStatusResponse
    {
        public UserSummaryDto user { get; set; } = default!;
    }

    public sealed class UserSummaryDto
    {
        public int id { get; set; }
        public string? name { get; set; }
        public string email { get; set; } = default!;
        public int m_user_type_id { get; set; }
        public string? country_code { get; set; } // Added this field
        public int? company_id { get; set; }
        public List<string> enabled_features { get; set; } = new();
    }
}
