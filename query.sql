-- name: UpsertBlogPost :exec
INSERT INTO blog_posts (blog_platform, post_url, path, published_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (post_url) DO UPDATE SET
    path = EXCLUDED.path,
    published_at = EXCLUDED.published_at,
    updated_at = current_timestamp;

-- name: GetBlogPostByPostUrl :one
SELECT blog_platform, post_url, path, published_at, created_at, updated_at
FROM blog_posts
WHERE post_url = $1;

-- name: ListBlogPosts :many
SELECT blog_platform, post_url, path, published_at, created_at, updated_at
FROM blog_posts
WHERE (sqlc.narg('cursor')::text IS NULL OR path > sqlc.narg('cursor')::text)
ORDER BY path
LIMIT $1;

-- name: CountBlogPosts :one
SELECT count(*) FROM blog_posts;

-- name: DeleteBlogPostsByBundlePath :execresult
DELETE FROM blog_posts WHERE path LIKE $1 || '?%';
