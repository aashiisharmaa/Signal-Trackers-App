using System.Security.Claims;
using Microsoft.AspNetCore.Http;

namespace SignalTracker.Services
{
    public class UserScopeService
    {
        private readonly IHttpContextAccessor _httpContextAccessor;

        public UserScopeService(IHttpContextAccessor httpContextAccessor)
        {
            _httpContextAccessor = httpContextAccessor;
        }

        // Constants for Roles
        public const int ROLE_SUPER_ADMIN = 3;
        public const int ROLE_ADMIN = 2;
        public const int ROLE_USER = 1;

        /// <summary>
        /// Data access is isolated per authenticated user. Company/global data scopes
        /// must not be used for dashboard/log/project reads.
        /// </summary>
        public int GetTargetCompanyId(ClaimsPrincipal user, int? requestedCompanyId)
        {
            return 0;
        }
        public bool IsSuperAdmin(ClaimsPrincipal user)
        {
            return false;
        }

        public int GetCurrentUserId(ClaimsPrincipal user)
        {
            return GetIntClaim(user, "UserId")
                ?? _httpContextAccessor.HttpContext?.Session?.GetInt32("UserID")
                ?? ParseInt(_httpContextAccessor.HttpContext?.Session?.GetString("UserID"))
                ?? 0;
        }

        internal int GetCompanyId(ClaimsPrincipal user)
        {
            throw new NotImplementedException();
        }

        private static string? GetStringClaim(ClaimsPrincipal user, params string[] claimTypes)
        {
            foreach (var claimType in claimTypes)
            {
                var value = user.FindFirst(claimType)?.Value;
                if (!string.IsNullOrWhiteSpace(value))
                    return value;
            }

            return null;
        }

        private static int? GetIntClaim(ClaimsPrincipal user, params string[] claimTypes)
        {
            var raw = GetStringClaim(user, claimTypes);
            return ParseInt(raw);
        }

        private static int? ParseInt(string? value)
        {
            return int.TryParse(value, out var parsed) ? parsed : null;
        }
    }
}
