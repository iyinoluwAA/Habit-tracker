use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::models::{User, UserRole};

#[derive(Debug, Clone)]
pub struct DBClient {
    pool: Pool<Postgres>,
}

impl DBClient {
    pub fn new(pool: Pool<Postgres>) -> Self {
        DBClient { pool }
    }
}

/* ---------------------------
   Existing User helpers
   --------------------------- */
#[async_trait]
pub trait UserExt {
    async fn get_user(
        &self,
        user_id: Option<Uuid>,
        name: Option<&str>,
        email: Option<&str>,
        token: Option<&str>,
    ) -> Result<Option<User>, sqlx::Error>;

    async fn get_users(
        &self,
        page: u32,
        limit: usize,
    ) -> Result<Vec<User>, sqlx::Error>;

    async fn save_user<T: Into<String> + Send>(
        &self,
        name: T,
        email: T,
        password: T,
        verification_token: T,
        token_expires_at: DateTime<Utc>,
    ) -> Result<User, sqlx::Error>;

    async fn get_user_count(&self) -> Result<i64, sqlx::Error>;

    async fn update_user_name<T: Into<String> + Send>(
        &self,
        user_id: Uuid,
        name: T,
    ) -> Result<User, sqlx::Error>;

    async fn update_user_role(
        &self,
        user_id: Uuid,
        role: UserRole,
    ) -> Result<User, sqlx::Error>;

    async fn update_user_password(
        &self,
        user_id: Uuid,
        password: String,
    ) -> Result<User, sqlx::Error>;

    async fn verifed_token(
        &self,
        token: &str,
    ) -> Result<(), sqlx::Error>;

    async fn add_verifed_token(
        &self,
        user_id: Uuid,
        token: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error>;
}

#[async_trait]
impl UserExt for DBClient {
    // paste your existing user methods here unchanged (kept verbatim)
    // BEGIN existing user methods
    async fn get_user(
        &self,
        user_id: Option<Uuid>,
        name: Option<&str>,
        email: Option<&str>,
        token: Option<&str>,
    ) -> Result<Option<User>, sqlx::Error> {
        let mut user: Option<User> = None;

        if let Some(user_id) = user_id {
            user = sqlx::query_as!(
                User,
                r#"SELECT id, name, email, password, verified, created_at, updated_at, verification_token, token_expires_at, role as "role: UserRole" FROM users WHERE id = $1"#,
                user_id
            ).fetch_optional(&self.pool).await?;
        } else if let Some(name) = name {
            user = sqlx::query_as!(
                User,
                r#"SELECT id, name, email, password, verified, created_at, updated_at, verification_token, token_expires_at, role as "role: UserRole" FROM users WHERE name = $1"#,
                name
            ).fetch_optional(&self.pool).await?;
        } else if let Some(email) = email {
            user = sqlx::query_as!(
                User,
                r#"SELECT id, name, email, password, verified, created_at, updated_at, verification_token, token_expires_at, role as "role: UserRole" FROM users WHERE email = $1"#,
                email
            ).fetch_optional(&self.pool).await?;
        } else if let Some(token) = token {
            user = sqlx::query_as!(
                User,
                r#"
                SELECT id, name, email, password, verified, created_at, updated_at, verification_token, token_expires_at, role as "role: UserRole"
                FROM users
                WHERE verification_token = $1"#,
                token
            )
            .fetch_optional(&self.pool)
            .await?;
        }

        Ok(user)
    }

    async fn get_users(
        &self,
        page: u32,
        limit: usize,
    ) -> Result<Vec<User>, sqlx::Error> {
        let offset = (page - 1) * limit as u32;

        let users = sqlx::query_as!(
            User,
            r#"SELECT id, name, email, password, verified, created_at, updated_at, verification_token, token_expires_at, role as "role: UserRole" FROM users
            ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
            limit as i64,
            offset as i64,
        ).fetch_all(&self.pool)
        .await?;

        Ok(users)
    }

    async fn save_user<T: Into<String> + Send>(
        &self,
        name: T,
        email: T,
        password: T,
        verification_token: T,
        token_expires_at: DateTime<Utc>,
    ) -> Result<User, sqlx::Error> {
        let user = sqlx::query_as!(
            User,
            r#"
            INSERT INTO users (name, email, password,verification_token, token_expires_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, name, email, password, verified, created_at, updated_at, verification_token, token_expires_at, role as "role: UserRole"
            "#,
            name.into(),
            email.into(),
            password.into(),
            verification_token.into(),
            token_expires_at
        ).fetch_one(&self.pool)
        .await?;
        Ok(user)
    }

    async fn get_user_count(&self) -> Result<i64, sqlx::Error> {
        let count = sqlx::query_scalar!(
            r#"SELECT COUNT(*) FROM users"#
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count.unwrap_or(0))
    }

    async fn update_user_name<T: Into<String> + Send>(
        &self,
        user_id: Uuid,
        new_name: T
    ) -> Result<User, sqlx::Error> {
        let user = sqlx::query_as!(
            User,
            r#"
            UPDATE users
            SET name = $1, updated_at = Now()
            WHERE id = $2
            RETURNING id, name, email, password, verified, created_at, updated_at, verification_token, token_expires_at, role as "role: UserRole"
            "#,
            new_name.into(),
            user_id
        ).fetch_one(&self.pool)
        .await?;

        Ok(user)
    }

    async fn update_user_role(
        &self,
        user_id: Uuid,
        new_role: UserRole
    ) -> Result<User, sqlx::Error> {
        let user = sqlx::query_as!(
            User,
            r#"
            UPDATE users
            SET role = $1, updated_at = Now()
            WHERE id = $2
            RETURNING id, name, email, password, verified, created_at, updated_at, verification_token, token_expires_at, role as "role: UserRole"
            "#,
            new_role as UserRole,
            user_id
        ).fetch_one(&self.pool)
        .await?;

        Ok(user)
    }

    async fn update_user_password(
        &self,
        user_id: Uuid,
        new_password: String
    ) -> Result<User, sqlx::Error> {
        let user = sqlx::query_as!(
            User,
            r#"
            UPDATE users
            SET password = $1, updated_at = Now()
            WHERE id = $2
            RETURNING id, name, email, password, verified, created_at, updated_at, verification_token, token_expires_at, role as "role: UserRole"
            "#,
            new_password,
            user_id
        ).fetch_one(&self.pool)
        .await?;

        Ok(user)
    }

    async fn verifed_token(
        &self,
        token: &str,
    ) -> Result<(), sqlx::Error> {
        let _ =sqlx::query!(
            r#"
            UPDATE users
            SET verified = true,
                updated_at = Now(),
                verification_token = NULL,
                token_expires_at = NULL
            WHERE verification_token = $1
            "#,
            token
        ).execute(&self.pool)
        .await;

        Ok(())
    }

    async fn add_verifed_token(
        &self,
        user_id: Uuid,
        token: &str,
        token_expires_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let _ = sqlx::query!(
            r#"
            UPDATE users
            SET verification_token = $1, token_expires_at = $2, updated_at = Now()
            WHERE id = $3
            "#,
            token,
            token_expires_at,
            user_id,
        ).execute(&self.pool)
        .await?;

        Ok(())
    }
    // END existing user methods
}

//
// New transcription job helpers
//
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TranscriptionJob {

    pub id: uuid::Uuid,
    pub user_id: Option<uuid::Uuid>,
    pub source_url: String,
    pub status: String,
    pub priority: i32,
    pub attempts: i32,
    pub max_attempts: i32,
    pub worker_id: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub transcript: Option<String>,
    pub transcript_format: Option<String>,
    pub duration_seconds: Option<i32>,
    pub size_bytes: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[async_trait]
pub trait TranscriptionExt {
    async fn enqueue_transcription(
        &self,
        user_id: Option<Uuid>,
        source_url: &str,
        priority: i32,
    ) -> Result<Uuid, sqlx::Error>;

    async fn claim_transcription_jobs(
        &self,
        worker_id: &str,
        limit: i64,
    ) -> Result<Vec<TranscriptionJob>, sqlx::Error>;

    async fn get_transcription_job(
        &self,
        job_id: Uuid,
    ) -> Result<Option<TranscriptionJob>, sqlx::Error>;

    async fn finalize_transcription_job(
        &self,
        job_id: Uuid,
        status: &str,
        transcript: Option<&str>,
        transcript_format: Option<&str>,
        last_error: Option<&str>,
        duration_seconds: Option<i32>,
        size_bytes: Option<i64>,
    ) -> Result<(), sqlx::Error>;
}

#[async_trait]
impl TranscriptionExt for DBClient {
    async fn enqueue_transcription(
        &self,
        user_id: Option<Uuid>,
        source_url: &str,
        priority: i32,
    ) -> Result<Uuid, sqlx::Error> {
        let id = sqlx::query_scalar!(
            r#"
            INSERT INTO transcription_jobs (user_id, source_url, priority)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
            user_id,
            source_url,
            priority as i32
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(id)
    }

    async fn claim_transcription_jobs(
        &self,
        worker_id: &str,
        limit: i64,
    ) -> Result<Vec<TranscriptionJob>, sqlx::Error> {
        let sql = r#"
            WITH cte AS (
              SELECT id FROM transcription_jobs
              WHERE status = 'enqueued'
              ORDER BY priority DESC, created_at ASC
              LIMIT $2
              FOR UPDATE SKIP LOCKED
            )
            UPDATE transcription_jobs
            SET status = 'processing',
                worker_id = $1,
                started_at = now(),
                attempts = attempts + 1,
                updated_at = now()
            WHERE id IN (SELECT id FROM cte)
            RETURNING id, user_id, source_url, status::text AS status, priority, attempts, max_attempts, worker_id, started_at, finished_at, last_error, transcript, transcript_format, duration_seconds, size_bytes, created_at, updated_at
        "#;

        let jobs: Vec<TranscriptionJob> = sqlx::query_as::<_, TranscriptionJob>(sql)
            .bind(worker_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        Ok(jobs)
    }

    async fn get_transcription_job(
        &self,
        job_id: Uuid,
    ) -> Result<Option<TranscriptionJob>, sqlx::Error> {
        let sql = r#"
            SELECT id, user_id, source_url, status::text AS status, priority, attempts, max_attempts, worker_id, started_at, finished_at, last_error, transcript, transcript_format, duration_seconds, size_bytes, created_at, updated_at
            FROM transcription_jobs
            WHERE id = $1
        "#;

        let job = sqlx::query_as::<_, TranscriptionJob>(sql)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(job)
    }

    async fn finalize_transcription_job(
        &self,
        job_id: Uuid,
        status: &str,
        transcript: Option<&str>,
        transcript_format: Option<&str>,
        last_error: Option<&str>,
        duration_seconds: Option<i32>,
        size_bytes: Option<i64>,
    ) -> Result<(), sqlx::Error> {
        let sql = r#"
            UPDATE transcription_jobs
            SET status = $2::transcription_status,
                transcript = $3,
                transcript_format = $4,
                last_error = $5,
                finished_at = now(),
                duration_seconds = $6,
                size_bytes = $7,
                updated_at = now()
            WHERE id = $1
        "#;

        let _ = sqlx::query(sql)
            .bind(job_id)
            .bind(status)
            .bind(transcript)
            .bind(transcript_format)
            .bind(last_error)
            .bind(duration_seconds)
            .bind(size_bytes)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
