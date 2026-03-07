-- links table stores URLs and its last accessed timestamp (for re-crawling purposes)

create table links (
                       url text not null unique primary key,
                       last_enqueued_at timestamp not null default current_timestamp,
                       created_at timestamp not null default current_timestamp
);
create index idx_links_last_enqueued_at on links (last_enqueued_at);
create index idx_links_created_at on links (created_at);

-- blog_users table stores users of the blog platforms for re-crawling purposes

create table blog_users (
                            blog_platform text not null,
                            user_id text not null,
                            last_enqueued_at timestamp not null default current_timestamp,
                            created_at timestamp not null default current_timestamp,
                            primary key (blog_platform, user_id)
);

create index idx_blog_users_last_enqueued_at on blog_users (last_enqueued_at);
create index idx_blog_users_created_at on blog_users (created_at);

-- blog_posts table stores blog posts' path and metadata

create table blog_posts (
                            blog_platform text not null,
                            post_url text not null,
                            path text not null,
                            published_at timestamp,
                            created_at timestamp not null default current_timestamp,
                            updated_at timestamp not null default current_timestamp,
                            primary key (post_url)
);

create index idx_blog_posts_created_at on blog_posts (created_at);
create index idx_blog_posts_updated_at on blog_posts (updated_at);

-- task queues stores backlog of tasks to be processed

-- fetch content
create table content_queue (
                               id bigserial primary key,
                               payload text not null,
                               enqueued_at timestamp not null default current_timestamp,
                               locked_until timestamp,
                               attempts integer not null default 0,
                               status text not null default 'waiting' -- waiting, processing, done, failed
);


create index idx_content_queue_enqueued_at on content_queue (enqueued_at);

-- fetch related profiles
create table profile_queue (
                               id bigserial primary key,
                               payload text not null,
                               enqueued_at timestamp not null default current_timestamp,
                               locked_until timestamp,
                               attempts integer not null default 0,
                               status text not null default 'waiting' -- waiting, processing, done, failed
);

create index idx_profile_queue_enqueued_at on profile_queue (enqueued_at);

-- fetch user data
create table user_queue (
                            id bigserial primary key,
                            payload text not null,
                            enqueued_at timestamp not null default current_timestamp,
                            locked_until timestamp,
                            attempts integer not null default 0,
                            status text not null default 'waiting' -- waiting, processing, done, failed
);

create index idx_user_queue_enqueued_at on user_queue (enqueued_at);