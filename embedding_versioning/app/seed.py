from app.db import init_db, connect
from app.docs import upsert_doc

def seed_docs():
    upsert_doc(1, "Reset password steps", "Send reset link; verify email; enforce policy.")
    upsert_doc(2, "Reset MFA for admin users", "Verify identity; revoke factors; re-enroll device.")
    upsert_doc(3, "Login loop after SSO", "Clear cookies; check IdP session; validate redirect URI.")
    upsert_doc(4, "Case: customer cannot login", "User locked out due to MFA expiry; reset and re-enroll.")
    upsert_doc(5, "Billing refund request", "Validate invoice; issue credit memo; confirm refund timing.")

def seed_golden():
    gold = [
        ("how to reset password", 1),
        ("reset mfa for admin", 2),
        ("sso redirect uri login loop", 3),
        ("customer cannot login mfa expired", 4),
        ("refund request invoice credit memo", 5),
    ]
    with connect() as conn:
        conn.execute("DELETE FROM golden_queries")
        for q, expected in gold:
            conn.execute("INSERT INTO golden_queries(query, expected_doc_id) VALUES (?, ?)", (q, expected))
        conn.commit()

def main():
    init_db()
    seed_docs()
    seed_golden()
    print("Seeded docs + golden queries.")

if __name__ == "__main__":
    main()
