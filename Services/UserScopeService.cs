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
        /// Determines the target Company ID based on the user's role.
        /// </summary>
        public int GetTargetCompanyId(ClaimsPrincipal user, int? requestedCompanyId)
        {
            int userRole = GetIntClaim(user, "UserTypeId", "m_user_type_id")
                ?? _httpContextAccessor.HttpContext?.Session?.GetInt32("UserType")
                ?? 0;

            // CRITICAL: This block allows Super Admins to see everything
            if (userRole == 3) // 3 = Super Admin
            {
                // If they requested a specific ID, return it. Otherwise return 0 (All).
                if (requestedCompanyId.HasValue && requestedCompanyId.Value > 0)
                    return requestedCompanyId.Value;

                return 0; // 0 means "Show All Data"
            }

            // For everyone else, force their own Company ID
            int resolvedCompanyId = GetIntClaim(user, "CompanyId", "company_id")
                ?? _httpContextAccessor.HttpContext?.Session?.GetInt32("CompanyId")
                ?? ParseInt(_httpContextAccessor.HttpContext?.Session?.GetString("CompanyId"))
                ?? 0;

            // Backward compatibility: if claim/session company context is missing,
            // accept explicit company_id from request to avoid blocking valid users.
            if (resolvedCompanyId <= 0 && requestedCompanyId.HasValue && requestedCompanyId.Value > 0)
                return requestedCompanyId.Value;

            return resolvedCompanyId;
        }
        public bool IsSuperAdmin(ClaimsPrincipal user)
        {
            int role = GetIntClaim(user, "UserTypeId", "m_user_type_id")
                ?? _httpContextAccessor.HttpContext?.Session?.GetInt32("UserType")
                ?? 0;

            return role == ROLE_SUPER_ADMIN;
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
