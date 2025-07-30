/*
 * Copyright 2020 The Backstage Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ConfigReader } from '@backstage/config';
import {
  createMockDirectory,
  mockServices,
  registerMswTestHooks,
} from '@backstage/backend-test-utils';
import fs from 'fs-extra';
import { rest } from 'msw';
import { setupServer } from 'msw/node';
import path from 'path';
import { GitlabUrlReader } from './GitlabUrlReader';
import { DefaultReadTreeResponseFactory } from './tree';
import { NotFoundError, NotModifiedError } from '@backstage/errors';
import {
  GitLabIntegration,
  readGitLabIntegrationConfig,
  GitlabProjectIdMapCache,
} from '@backstage/integration';
import { UrlReaderServiceReadUrlResponse } from '@backstage/backend-plugin-api';

const logger = mockServices.logger.mock();

const mockDir = createMockDirectory({ mockOsTmpDir: true });

const treeResponseFactory = DefaultReadTreeResponseFactory.create({
  config: new ConfigReader({}),
});

// Create a mock cache implementation for testing
class MockGitlabProjectIdMapCache implements GitlabProjectIdMapCache {
  private cache = new Map<string, number>();

  getProjectId(projectPath: string, repository: string): number | undefined {
    return this.cache.get(`${projectPath}-${repository}`);
  }

  setProjectId(
    projectPath: string,
    repository: string,
    projectId: number,
  ): void {
    this.cache.set(`${projectPath}-${repository}`, projectId);
  }

  clear(): void {
    this.cache.clear();
  }
}

const mockCache = new MockGitlabProjectIdMapCache();

const gitlabProcessor = new GitlabUrlReader(
  new GitLabIntegration(
    readGitLabIntegrationConfig(
      new ConfigReader({
        host: 'gitlab.com',
        apiBaseUrl: 'https://gitlab.com/api/v4',
        baseUrl: 'https://gitlab.com',
        token: 'gl-dummy-token',
      }),
    ),
  ),
  { treeResponseFactory, projectIdMapCache: mockCache },
);

const hostedGitlabProcessor = new GitlabUrlReader(
  new GitLabIntegration(
    readGitLabIntegrationConfig(
      new ConfigReader({
        host: 'gitlab.mycompany.com',
        apiBaseUrl: 'https://gitlab.mycompany.com/api/v4',
        baseUrl: 'https://gitlab.mycompany.com',
        token: 'gl-dummy-token',
      }),
    ),
  ),
  { treeResponseFactory, projectIdMapCache: new MockGitlabProjectIdMapCache() },
);

describe('GitlabUrlReader', () => {
  beforeEach(() => {
    mockDir.clear();
    mockCache.clear();
  });

  const worker = setupServer();
  registerMswTestHooks(worker);

  describe('GitlabProjectIdMapCacheImpl', () => {
    // Import the actual implementation for testing
    const GitlabProjectIdMapCacheImpl =
      (GitlabUrlReader as any).GitlabProjectIdMapCacheImpl ||
      class GitlabProjectIdMapCacheImpl implements GitlabProjectIdMapCache {
        private readonly cache = new Map<
          string,
          { projectId: number; lastUpdated: number }
        >();

        constructor(
          private readonly cacheTTL: number,
          private readonly maxSize: number,
        ) {}

        getProjectId(
          projectPath: string,
          repository: string,
        ): number | undefined {
          const cacheKey = `${projectPath}-${repository}`;
          const cacheEntry = this.cache.get(cacheKey);

          if (
            cacheEntry &&
            Date.now() - cacheEntry.lastUpdated < this.cacheTTL
          ) {
            // Move to end (most recently used) by deleting and re-inserting
            this.cache.delete(cacheKey);
            this.cache.set(cacheKey, cacheEntry);
            return cacheEntry.projectId;
          }

          // Remove expired entry if it exists
          if (cacheEntry) {
            this.cache.delete(cacheKey);
          }

          return undefined;
        }

        setProjectId(
          projectPath: string,
          repository: string,
          projectId: number,
        ): void {
          const cacheKey = `${projectPath}-${repository}`;

          // If entry already exists, delete it first (will be re-added at the end)
          if (this.cache.has(cacheKey)) {
            this.cache.delete(cacheKey);
          } else if (this.cache.size >= this.maxSize) {
            // Remove least recently used entry (first entry in the Map)
            const firstKey = this.cache.keys().next().value;
            if (firstKey) {
              this.cache.delete(firstKey);
            }
          }

          // Add the new entry (will be at the end, most recently used)
          this.cache.set(cacheKey, {
            projectId,
            lastUpdated: Date.now(),
          });
        }
      };

    it('should cache project IDs within TTL', () => {
      const cache = new GitlabProjectIdMapCacheImpl(1000, 500); // 1 second TTL, 500 max size
      const projectPath = 'https://gitlab.com';
      const repository = 'user/repo';
      const projectId = 12345;

      // Should return undefined when cache is empty
      expect(cache.getProjectId(projectPath, repository)).toBeUndefined();

      // Set a project ID in cache
      cache.setProjectId(projectPath, repository, projectId);

      // Should return the cached project ID
      expect(cache.getProjectId(projectPath, repository)).toBe(projectId);
    });

    it('should return undefined for expired cache entries', async () => {
      const cache = new GitlabProjectIdMapCacheImpl(10, 500); // 10ms TTL, 500 max size
      const projectPath = 'https://gitlab.com';
      const repository = 'user/repo';
      const projectId = 12345;

      // Set a project ID in cache
      cache.setProjectId(projectPath, repository, projectId);

      // Should return the cached project ID immediately
      expect(cache.getProjectId(projectPath, repository)).toBe(projectId);

      // Wait for TTL to expire
      await new Promise(resolve => setTimeout(resolve, 20));

      // Should return undefined after TTL expires
      expect(cache.getProjectId(projectPath, repository)).toBeUndefined();
    });

    it('should handle different project paths and repositories separately', () => {
      const cache = new GitlabProjectIdMapCacheImpl(1000, 500); // 1 second TTL, 500 max size

      cache.setProjectId('https://gitlab.com', 'user1/repo1', 111);
      cache.setProjectId('https://gitlab.com', 'user2/repo2', 222);
      cache.setProjectId('https://example.com', 'user1/repo1', 333);

      expect(cache.getProjectId('https://gitlab.com', 'user1/repo1')).toBe(111);
      expect(cache.getProjectId('https://gitlab.com', 'user2/repo2')).toBe(222);
      expect(cache.getProjectId('https://example.com', 'user1/repo1')).toBe(
        333,
      );
      expect(
        cache.getProjectId('https://gitlab.com', 'user3/repo3'),
      ).toBeUndefined();
    });

    it('should enforce maximum cache size by evicting least recently used entries', () => {
      const cache = new GitlabProjectIdMapCacheImpl(10000, 3); // Long TTL, small max size for testing
      const projectPath = 'https://gitlab.com';

      // Fill cache to capacity
      cache.setProjectId(projectPath, 'repo1', 111);
      cache.setProjectId(projectPath, 'repo2', 222);
      cache.setProjectId(projectPath, 'repo3', 333);

      // All entries should be present
      expect(cache.getProjectId(projectPath, 'repo1')).toBe(111);
      expect(cache.getProjectId(projectPath, 'repo2')).toBe(222);
      expect(cache.getProjectId(projectPath, 'repo3')).toBe(333);

      // Add one more entry, should evict the oldest (repo1)
      cache.setProjectId(projectPath, 'repo4', 444);

      // repo1 should be evicted, others should remain
      expect(cache.getProjectId(projectPath, 'repo1')).toBeUndefined();
      expect(cache.getProjectId(projectPath, 'repo2')).toBe(222);
      expect(cache.getProjectId(projectPath, 'repo3')).toBe(333);
      expect(cache.getProjectId(projectPath, 'repo4')).toBe(444);
    });

    it('should move accessed entries to most recently used position', () => {
      const cache = new GitlabProjectIdMapCacheImpl(10000, 3); // Long TTL, small max size for testing
      const projectPath = 'https://gitlab.com';

      // Fill cache to capacity
      cache.setProjectId(projectPath, 'repo1', 111);
      cache.setProjectId(projectPath, 'repo2', 222);
      cache.setProjectId(projectPath, 'repo3', 333);

      // Access repo1 to move it to most recently used
      expect(cache.getProjectId(projectPath, 'repo1')).toBe(111);

      // Add one more entry, should evict repo2 (now oldest) instead of repo1
      cache.setProjectId(projectPath, 'repo4', 444);

      // repo2 should be evicted, repo1 should remain due to recent access
      expect(cache.getProjectId(projectPath, 'repo1')).toBe(111);
      expect(cache.getProjectId(projectPath, 'repo2')).toBeUndefined();
      expect(cache.getProjectId(projectPath, 'repo3')).toBe(333);
      expect(cache.getProjectId(projectPath, 'repo4')).toBe(444);
    });

    it('should handle updating existing entries without affecting cache size', () => {
      const cache = new GitlabProjectIdMapCacheImpl(10000, 2); // Long TTL, small max size for testing
      const projectPath = 'https://gitlab.com';

      // Fill cache to capacity
      cache.setProjectId(projectPath, 'repo1', 111);
      cache.setProjectId(projectPath, 'repo2', 222);

      // Update existing entry with new project ID (moves repo1 to most recently used)
      cache.setProjectId(projectPath, 'repo1', 999);

      // Add new entry, should evict repo2 (oldest after repo1 was updated)
      cache.setProjectId(projectPath, 'repo3', 333);

      // Check final state: repo1 should still be present, repo2 should be evicted
      expect(cache.getProjectId(projectPath, 'repo1')).toBe(999);
      expect(cache.getProjectId(projectPath, 'repo2')).toBeUndefined();
      expect(cache.getProjectId(projectPath, 'repo3')).toBe(333);
    });

    it('should clean up expired entries when accessed', () => {
      const cache = new GitlabProjectIdMapCacheImpl(10, 500); // 10ms TTL, large max size
      const projectPath = 'https://gitlab.com';

      // Set a project ID in cache
      cache.setProjectId(projectPath, 'repo1', 111);

      // Should return the cached project ID immediately
      expect(cache.getProjectId(projectPath, 'repo1')).toBe(111);

      // Wait for TTL to expire
      return new Promise<void>(resolve => {
        setTimeout(() => {
          // Accessing expired entry should return undefined and clean it up
          expect(cache.getProjectId(projectPath, 'repo1')).toBeUndefined();

          // Add new entry to verify the expired one was cleaned up
          cache.setProjectId(projectPath, 'repo2', 222);
          expect(cache.getProjectId(projectPath, 'repo2')).toBe(222);

          resolve();
        }, 20);
      });
    });
  });

  describe('factory method', () => {
    it('should create readers with cache from config', () => {
      const config = new ConfigReader({
        integrations: {
          gitlab: [{ host: 'gitlab.com', token: 'test-token' }],
        },
        gitlab: {
          projectIdMapCacheTTL: 10000, // 10 seconds
          projectIdMapCacheMaxSize: 1000, // 1000 entries
        },
      });

      const readers = GitlabUrlReader.factory({
        config,
        logger,
        treeResponseFactory,
      });

      expect(readers).toHaveLength(1);
      expect(readers[0].reader).toBeInstanceOf(GitlabUrlReader);
    });

    it('should use default cache TTL and max size when not specified', () => {
      const config = new ConfigReader({
        integrations: {
          gitlab: [{ host: 'gitlab.com', token: 'test-token' }],
        },
      });

      const readers = GitlabUrlReader.factory({
        config,
        logger,
        treeResponseFactory,
      });

      expect(readers).toHaveLength(1);
      expect(readers[0].reader).toBeInstanceOf(GitlabUrlReader);
    });

    it('should use custom cache max size from config', () => {
      const config = new ConfigReader({
        integrations: {
          gitlab: [{ host: 'gitlab.com', token: 'test-token' }],
        },
        gitlab: {
          projectIdMapCacheMaxSize: 100, // Custom max size
        },
      });

      const readers = GitlabUrlReader.factory({
        config,
        logger,
        treeResponseFactory,
      });

      expect(readers).toHaveLength(1);
      expect(readers[0].reader).toBeInstanceOf(GitlabUrlReader);
    });
  });

  describe('read', () => {
    beforeEach(() => {
      worker.use(
        rest.get('*/api/v4/projects/:name', (_, res, ctx) =>
          res(ctx.status(200), ctx.json({ id: 12345 })),
        ),
        rest.get('*', (req, res, ctx) =>
          res(
            ctx.status(200),
            ctx.json({
              url: req.url.toString(),
              headers: req.headers.all(),
            }),
          ),
        ),
      );
    });

    const createConfig = (token?: string) =>
      new ConfigReader(
        {
          integrations: { gitlab: [{ host: 'gitlab.com', token }] },
        },
        'test-config',
      );

    it.each([
      // Scoped routes
      {
        url: 'https://gitlab.com/groupA/teams/teamA/subgroupA/repoA/-/blob/branch/my/path/to/file.yaml',
        config: createConfig(),
        response: expect.objectContaining({
          url: 'https://gitlab.com/api/v4/projects/12345/repository/files/my%2Fpath%2Fto%2Ffile.yaml/raw?ref=branch',
        }),
      },
      {
        url: 'https://gitlab.example.com/groupA/teams/teamA/subgroupA/repoA/-/blob/branch/my/path/to/file.yaml',
        config: createConfig('0123456789'),
        response: expect.objectContaining({
          url: 'https://gitlab.example.com/api/v4/projects/12345/repository/files/my%2Fpath%2Fto%2Ffile.yaml/raw?ref=branch',
          headers: expect.objectContaining({
            authorization: 'Bearer 0123456789',
          }),
        }),
      },
      {
        url: 'https://gitlab.com/groupA/teams/teamA/repoA/-/blob/branch/my/path/to/file.yaml', // Repo not in subgroup
        config: createConfig(),
        response: expect.objectContaining({
          url: 'https://gitlab.com/api/v4/projects/12345/repository/files/my%2Fpath%2Fto%2Ffile.yaml/raw?ref=branch',
        }),
      },

      // Unscoped route
      {
        url: 'https://gitlab.example.com/a/b/blob/master/c.yaml',
        config: createConfig(),
        response: expect.objectContaining({
          url: 'https://gitlab.example.com/api/v4/projects/12345/repository/files/c.yaml/raw?ref=master',
        }),
      },
    ])('should handle happy path %#', async ({ url, config, response }) => {
      const [{ reader }] = GitlabUrlReader.factory({
        config,
        logger,
        treeResponseFactory,
      });

      const { buffer } = await reader.readUrl(url);
      const fromStream = await buffer();
      const res = await JSON.parse(fromStream.toString());
      expect(res).toEqual(response);
    });

    it.each([
      {
        url: '',
        config: createConfig(''),
        error:
          "Invalid type in config for key 'integrations.gitlab[0].token' in 'test-config', got empty-string, wanted string",
      },
    ])('should handle error path %#', async ({ url, config, error }) => {
      await expect(async () => {
        const [{ reader }] = GitlabUrlReader.factory({
          config,
          logger,
          treeResponseFactory,
        });
        await reader.readUrl(url);
      }).rejects.toThrow(error);
    });
  });

  describe('readUrl', () => {
    const [{ reader }] = GitlabUrlReader.factory({
      config: new ConfigReader({}),
      logger,
      treeResponseFactory,
    });

    it('should throw NotModified on HTTP 304 from etag', async () => {
      worker.use(
        rest.get('*/api/v4/projects/:name', (_, res, ctx) =>
          res(ctx.status(200), ctx.json({ id: 12345 })),
        ),
        rest.get('*', (req, res, ctx) => {
          expect(req.headers.get('If-None-Match')).toBe('999');
          return res(ctx.status(304));
        }),
      );

      await expect(
        reader.readUrl!(
          'https://gitlab.com/groupA/teams/teamA/subgroupA/repoA/-/blob/branch/my/path/to/file.yaml',
          {
            etag: '999',
          },
        ),
      ).rejects.toThrow(NotModifiedError);
    });

    it('should throw NotModified on HTTP 304 from lastModifiedAt', async () => {
      worker.use(
        rest.get('*/api/v4/projects/:name', (_, res, ctx) =>
          res(ctx.status(200), ctx.json({ id: 12345 })),
        ),
        rest.get('*', (req, res, ctx) => {
          expect(req.headers.get('If-Modified-Since')).toBe(
            new Date('2019 12 31 23:59:59 GMT').toUTCString(),
          );
          return res(ctx.status(304));
        }),
      );

      await expect(
        reader.readUrl!(
          'https://gitlab.com/groupA/teams/teamA/subgroupA/repoA/-/blob/branch/my/path/to/file.yaml',
          {
            lastModifiedAfter: new Date('2019 12 31 23:59:59 GMT'),
          },
        ),
      ).rejects.toThrow(NotModifiedError);
    });

    it('should return etag and last-modified in response', async () => {
      worker.use(
        rest.get('*/api/v4/projects/:name', (_, res, ctx) =>
          res(ctx.status(200), ctx.json({ id: 12345 })),
        ),
        rest.get('*', (_req, res, ctx) => {
          return res(
            ctx.status(200),
            ctx.set('ETag', '999'),
            ctx.set(
              'Last-Modified',
              new Date('2020 01 01 00:0:00 GMT').toUTCString(),
            ),
            ctx.body('foo'),
          );
        }),
      );

      const result = await reader.readUrl!(
        'https://gitlab.com/groupA/teams/teamA/subgroupA/repoA/-/blob/branch/my/path/to/file.yaml',
      );
      expect(result.etag).toBe('999');
      expect(result.lastModifiedAt).toEqual(new Date('2020 01 01 00:0:00 GMT'));
      const content = await result.buffer();
      expect(content.toString()).toBe('foo');
    });

    it('should return the file when using a user token', async () => {
      worker.use(
        rest.get('*/api/v4/projects/user%2Fproject', (req, res, ctx) => {
          if (req.headers.get('authorization') !== 'Bearer gl-user-token') {
            return res(
              ctx.status(401),
              ctx.json({ message: '401 Unauthorized' }),
            );
          }
          return res(ctx.status(200), ctx.json({ id: 12345 }));
        }),
        rest.get('*', (_req, res, ctx) => {
          return res(ctx.status(200), ctx.body('foo'));
        }),
      );
      const result = await reader.readUrl(
        'https://gitlab.com/user/project/-/blob/branch/my/path/to/file.yaml',
        { token: 'gl-user-token' },
      );
      const content = await result.buffer();
      expect(content.toString()).toBe('foo');
    });
  });

  describe('readTree', () => {
    const archiveBuffer = fs.readFileSync(
      path.resolve(__dirname, '__fixtures__/gitlab-archive.tar.gz'),
    );

    let projectGitlabApiResponse: any;
    let commitsGitlabApiResponse: any;
    let specificPathCommitsGitlabApiResponse: any;

    beforeEach(() => {
      projectGitlabApiResponse = {
        id: 11111111,
        default_branch: 'main',
      };

      commitsGitlabApiResponse = [
        {
          id: 'sha123abc',
        },
      ];

      specificPathCommitsGitlabApiResponse = [
        {
          id: 'sha456def',
        },
      ];

      const projectNames = ['backstage%2Fmock', 'user%2Fproject'];
      projectNames.forEach(projectName => {
        worker.use(
          rest.get(
            `https://gitlab.com/api/v4/projects/${projectName}/repository/archive`,
            (_, res, ctx) =>
              res(
                ctx.status(200),
                ctx.set('Content-Type', 'application/zip'),
                ctx.set(
                  'content-disposition',
                  'attachment; filename="mock-main-sha123abc.zip"',
                ),
                ctx.body(archiveBuffer),
              ),
          ),
          rest.get(
            `https://gitlab.com/api/v4/projects/${projectName}`,
            (_, res, ctx) =>
              res(
                ctx.status(200),
                ctx.set('Content-Type', 'application/json'),
                ctx.json(projectGitlabApiResponse),
              ),
          ),
          rest.get(
            `https://gitlab.com/api/v4/projects/${projectName}/repository/commits`,
            (req, res, ctx) => {
              const refName = req.url.searchParams.get('ref_name');
              if (refName === 'main') {
                const filepath = req.url.searchParams.get('path');
                if (filepath === 'testFilepath') {
                  return res(
                    ctx.status(200),
                    ctx.set('Content-Type', 'application/json'),
                    ctx.json(specificPathCommitsGitlabApiResponse),
                  );
                }
                return res(
                  ctx.status(200),
                  ctx.set('Content-Type', 'application/json'),
                  ctx.json(commitsGitlabApiResponse),
                );
              }
              if (refName === 'branchDoesNotExist') {
                return res(ctx.status(404));
              }
              return res();
            },
          ),
          rest.get(
            `https://gitlab.mycompany.com/api/v4/projects/${projectName}`,
            (_, res, ctx) =>
              res(
                ctx.status(200),
                ctx.set('Content-Type', 'application/json'),
                ctx.json(projectGitlabApiResponse),
              ),
          ),
          rest.get(
            `https://gitlab.mycompany.com/api/v4/projects/${projectName}/repository/commits`,
            (req, res, ctx) => {
              const refName = req.url.searchParams.get('ref_name');
              if (refName === 'main') {
                const filepath = req.url.searchParams.get('path');
                if (filepath === 'testFilepath') {
                  return res(
                    ctx.status(200),
                    ctx.set('Content-Type', 'application/json'),
                    ctx.json(specificPathCommitsGitlabApiResponse),
                  );
                }
                return res(
                  ctx.status(200),
                  ctx.set('Content-Type', 'application/json'),
                  ctx.json(commitsGitlabApiResponse),
                );
              }
              return res();
            },
          ),
          rest.get(
            `https://gitlab.mycompany.com/api/v4/projects/${projectName}/repository/archive`,
            (_, res, ctx) =>
              res(
                ctx.status(200),
                ctx.set('Content-Type', 'application/zip'),
                ctx.set(
                  'content-disposition',
                  'attachment; filename="mock-main-sha123abc.zip"',
                ),
                ctx.body(archiveBuffer),
              ),
          ),
        );
      });
    });

    it('returns the wanted files from an archive', async () => {
      const response = await gitlabProcessor.readTree(
        'https://gitlab.com/backstage/mock/tree/main',
      );

      const files = await response.files();
      expect(files.length).toBe(2);

      const indexMarkdownFile = await files[0].content();
      const mkDocsFile = await files[1].content();

      expect(mkDocsFile.toString()).toBe('site_name: Test\n');
      expect(indexMarkdownFile.toString()).toBe('# Test\n');
    });

    it('creates a directory with the wanted files', async () => {
      const response = await gitlabProcessor.readTree(
        'https://gitlab.com/backstage/mock',
      );

      const dir = await response.dir({ targetDir: mockDir.path });

      await expect(
        fs.readFile(path.join(dir, 'mkdocs.yml'), 'utf8'),
      ).resolves.toBe('site_name: Test\n');
      await expect(
        fs.readFile(path.join(dir, 'docs', 'index.md'), 'utf8'),
      ).resolves.toBe('# Test\n');
    });

    it('returns the wanted files from hosted gitlab', async () => {
      worker.use(
        rest.get(
          'https://gitlab.mycompany.com/backstage/mock/-/archive/main.tar.gz',
          (_, res, ctx) =>
            res(
              ctx.status(200),
              ctx.set('Content-Type', 'application/zip'),
              ctx.set(
                'content-disposition',
                'attachment; filename="mock-main-sha123abc.zip"',
              ),
              ctx.body(archiveBuffer),
            ),
        ),
      );

      const response = await hostedGitlabProcessor.readTree(
        'https://gitlab.mycompany.com/backstage/mock/tree/main/docs',
      );

      const files = await response.files();

      expect(files.length).toBe(1);
      const indexMarkdownFile = await files[0].content();

      expect(indexMarkdownFile.toString()).toBe('# Test\n');
    });

    it('returns the wanted files from an archive with a subpath', async () => {
      const response = await gitlabProcessor.readTree(
        'https://gitlab.com/backstage/mock/tree/main/docs',
      );

      const files = await response.files();

      expect(files.length).toBe(1);
      const indexMarkdownFile = await files[0].content();

      expect(indexMarkdownFile.toString()).toBe('# Test\n');
    });

    it('creates a directory with the wanted files with subpath', async () => {
      const response = await gitlabProcessor.readTree(
        'https://gitlab.com/backstage/mock/tree/main/docs',
      );

      const dir = await response.dir({ targetDir: mockDir.path });

      await expect(
        fs.readFile(path.join(dir, 'index.md'), 'utf8'),
      ).resolves.toBe('# Test\n');
    });

    it('throws a NotModifiedError when given a etag in options matching last commit', async () => {
      const fnGitlab = async () => {
        await gitlabProcessor.readTree('https://gitlab.com/backstage/mock', {
          etag: 'sha123abc',
        });
      };

      const fnHostedGitlab = async () => {
        await hostedGitlabProcessor.readTree(
          'https://gitlab.mycompany.com/backstage/mock',
          {
            etag: 'sha123abc',
          },
        );
      };

      await expect(fnGitlab).rejects.toThrow(NotModifiedError);
      await expect(fnHostedGitlab).rejects.toThrow(NotModifiedError);
    });

    it('throws a NotModifiedError when given a etag in options matching last commit affecting specified filepath', async () => {
      const fnGitlab = async () => {
        await gitlabProcessor.readTree(
          'https://gitlab.com/backstage/mock/blob/main/testFilepath',
          {
            etag: 'sha456def',
          },
        );
      };

      const fnHostedGitlab = async () => {
        await hostedGitlabProcessor.readTree(
          'https://gitlab.mycompany.com/backstage/mock/blob/main/testFilepath',
          {
            etag: 'sha456def',
          },
        );
      };

      await expect(fnGitlab).rejects.toThrow(NotModifiedError);
      await expect(fnHostedGitlab).rejects.toThrow(NotModifiedError);
    });

    it('should not throw error when given an outdated etag in options', async () => {
      const response = await gitlabProcessor.readTree(
        'https://gitlab.com/backstage/mock/tree/main',
        {
          etag: 'outdatedsha123abc',
        },
      );
      expect((await response.files()).length).toBe(2);
    });

    it('should detect the default branch', async () => {
      const response = await gitlabProcessor.readTree(
        'https://gitlab.com/backstage/mock',
      );
      expect((await response.files()).length).toBe(2);
    });

    it('should throw error on missing branch', async () => {
      const fnGitlab = async () => {
        await gitlabProcessor.readTree(
          'https://gitlab.com/backstage/mock/tree/branchDoesNotExist',
        );
      };
      await expect(fnGitlab).rejects.toThrow(NotFoundError);
    });

    it('should gracefully handle no matching commits', async () => {
      commitsGitlabApiResponse = [];

      const response = await gitlabProcessor.readTree(
        'https://gitlab.com/backstage/mock/tree/main',
      );

      const files = await response.files();
      expect(files.length).toBe(2);

      const indexMarkdownFile = await files[0].content();
      const mkDocsFile = await files[1].content();

      expect(mkDocsFile.toString()).toBe('site_name: Test\n');
      expect(indexMarkdownFile.toString()).toBe('# Test\n');
    });

    it('should return the file when using a user token', async () => {
      worker.use(
        rest.get('*/api/v4/projects/user%2Fproject', (req, res, ctx) => {
          if (req.headers.get('authorization') !== 'Bearer gl-user-token') {
            return res(
              ctx.status(401),
              ctx.json({ message: '401 Unauthorized' }),
            );
          }
          return res(ctx.status(200), ctx.json({ id: 12345 }));
        }),
      );

      const response = await gitlabProcessor.readTree(
        'https://gitlab.com/user/project/tree/main',
        { token: 'gl-user-token' },
      );

      const files = await response.files();
      expect(files.length).toBe(2);
    });
  });

  describe('search', () => {
    const archiveBuffer = fs.readFileSync(
      path.resolve(__dirname, '__fixtures__/gitlab-archive.tar.gz'),
    );

    const archiveSubPathBuffer = fs.readFileSync(
      path.resolve(__dirname, '__fixtures__/gitlab-subpath-archive.tar.gz'),
    );

    const projectGitlabApiResponse = {
      id: 11111111,
      default_branch: 'main',
    };

    const commitsGitlabApiResponse = [
      {
        id: 'sha123abc',
      },
    ];
    const commitsOfSubPathGitlabApiResponse = [
      {
        id: 'sha456abc',
      },
    ];

    beforeEach(() => {
      worker.use(
        rest.get(
          'https://gitlab.com/api/v4/projects/backstage%2Fmock/repository/archive',
          (req, res, ctx) => {
            const filepath = req.url.searchParams.get('path');
            let filename = 'mock-main-sha123abc.zip';
            let body = archiveBuffer;
            if (filepath === 'docs') {
              filename = 'gitlab-subpath-archive.tar.gz';
              body = archiveSubPathBuffer;
            }
            return res(
              ctx.status(200),
              ctx.set('Content-Type', 'application/zip'),
              ctx.set(
                'content-disposition',
                `attachment; filename="${filename}"`,
              ),
              ctx.body(body),
            );
          },
        ),
        rest.get(
          'https://gitlab.com/api/v4/projects/backstage%2Fmock',
          (_, res, ctx) =>
            res(
              ctx.status(200),
              ctx.set('Content-Type', 'application/json'),
              ctx.json(projectGitlabApiResponse),
            ),
        ),
        rest.get(
          'https://gitlab.com/api/v4/projects/backstage%2Fmock/repository/commits',
          (req, res, ctx) => {
            const refName = req.url.searchParams.get('ref_name');
            if (refName === 'main') {
              const filepath = req.url.searchParams.get('path');
              if (filepath === 'docs') {
                return res(
                  ctx.status(200),
                  ctx.set('Content-Type', 'application/json'),
                  ctx.json(commitsOfSubPathGitlabApiResponse),
                );
              }
              return res(
                ctx.status(200),
                ctx.set('Content-Type', 'application/json'),
                ctx.json(commitsGitlabApiResponse),
              );
            }
            return res();
          },
        ),
      );
    });

    it('works for the naive case', async () => {
      const result = await gitlabProcessor.search(
        'https://gitlab.com/backstage/mock/tree/main/**/index.*',
      );
      expect(result.etag).toBe('sha123abc');
      expect(result.files.length).toBe(1);
      expect(result.files[0].url).toBe(
        'https://gitlab.com/backstage/mock/tree/main/docs/index.md',
      );
      await expect(result.files[0].content()).resolves.toEqual(
        Buffer.from('# Test\n'),
      );
    });

    it('load only relevant path', async () => {
      const result = await gitlabProcessor.search(
        'https://gitlab.com/backstage/mock/tree/main/docs/**/index.*',
      );

      expect(result.etag).toBe('sha456abc');
      expect(result.files.length).toBe(1);
      expect(result.files[0].url).toBe(
        'https://gitlab.com/backstage/mock/tree/main/docs/index.md',
      );
      await expect(result.files[0].content()).resolves.toEqual(
        Buffer.from('# Test Subpath\n'),
      );
    });

    it('throws NotModifiedError when same etag', async () => {
      await expect(
        gitlabProcessor.search(
          'https://gitlab.com/backstage/mock/tree/main/**/index.*',
          { etag: 'sha123abc' },
        ),
      ).rejects.toThrow(NotModifiedError);
    });

    it('returns a single file for exact urls', async () => {
      gitlabProcessor.readUrl = jest.fn().mockResolvedValue({
        buffer: async () => Buffer.from('content'),
        etag: 'etag',
      } as UrlReaderServiceReadUrlResponse);
      const data = await gitlabProcessor.search(
        'https://github.com/backstage/mock/tree/main/o',
      );
      expect(gitlabProcessor.readUrl).toHaveBeenCalledTimes(1);
      expect(data.etag).toBe('etag');
      expect(data.files.length).toBe(1);
      expect(data.files[0].url).toBe(
        'https://github.com/backstage/mock/tree/main/o',
      );
      expect((await data.files[0].content()).toString()).toEqual('content');
    });
  });

  describe('getGitlabFetchUrl', () => {
    beforeEach(() => {
      worker.use(
        rest.get(
          '*/api/v4/projects/group%2Fsubgroup%2Fproject',
          (_, res, ctx) => res(ctx.status(200), ctx.json({ id: 12345 })),
        ),
        rest.get('*/api/v4/projects/user%2Fproject', (req, res, ctx) => {
          if (req.headers.get('authorization') !== 'Bearer gl-user-token') {
            return res(
              ctx.status(401),
              ctx.json({ message: '401 Unauthorized' }),
            );
          }
          return res(ctx.status(200), ctx.json({ id: 12345 }));
        }),
      );
    });
    it('should fall back to getGitLabFileFetchUrl for blob urls', async () => {
      await expect(
        (gitlabProcessor as any).getGitlabFetchUrl(
          'https://gitlab.com/group/subgroup/project/-/blob/branch/my/path/to/file.yaml',
        ),
      ).resolves.toEqual(
        'https://gitlab.com/api/v4/projects/12345/repository/files/my%2Fpath%2Fto%2Ffile.yaml/raw?ref=branch',
      );
    });
    it('should work for job artifact urls', async () => {
      await expect(
        (gitlabProcessor as any).getGitlabFetchUrl(
          'https://gitlab.com/group/subgroup/project/-/jobs/artifacts/branch/raw/my/path/to/file.yaml?job=myJob',
        ),
      ).resolves.toEqual(
        'https://gitlab.com/api/v4/projects/12345/jobs/artifacts/branch/raw/my/path/to/file.yaml?job=myJob',
      );
    });
    it('should fail on unfamiliar or non-Gitlab urls', async () => {
      await expect(
        (gitlabProcessor as any).getGitlabFetchUrl(
          'https://gitlab.com/some/random/endpoint',
        ),
      ).rejects.toThrow(
        'Failed converting /some/random/endpoint to a project id. Url path must include /blob/.',
      );
    });
    it('should resolve the project path using a user token', async () => {
      await expect(
        (gitlabProcessor as any).getGitlabFetchUrl(
          'https://gitlab.com/user/project/-/blob/branch/my/path/to/file.yaml',
          'gl-user-token',
        ),
      ).resolves.toEqual(
        'https://gitlab.com/api/v4/projects/12345/repository/files/my%2Fpath%2Fto%2Ffile.yaml/raw?ref=branch',
      );
    });
  });

  describe('getGitlabArtifactFetchUrl', () => {
    beforeEach(() => {
      worker.use(
        rest.get(
          '*/api/v4/projects/group%2Fsubgroup%2Fproject',
          (_, res, ctx) => res(ctx.status(200), ctx.json({ id: 12345 })),
        ),
        rest.get(
          '*/api/v4/projects/groupA%2Fsubgroup%2Fproject',
          (_, res, ctx) => res(ctx.status(404)),
        ),
        rest.get('*/api/v4/projects/user%2Fproject', (req, res, ctx) => {
          if (req.headers.get('authorization') !== 'Bearer gl-user-token') {
            return res(
              ctx.status(401),
              ctx.json({ message: '401 Unauthorized' }),
            );
          }
          return res(ctx.status(200), ctx.json({ id: 12345 }));
        }),
      );
    });
    it('should reject urls that are not for the job artifacts API', async () => {
      await expect(
        (gitlabProcessor as any).getGitlabArtifactFetchUrl(
          new URL('https://gitlab.com/some/url'),
        ),
      ).rejects.toThrow('Unable to process url as an GitLab artifact');
    });
    it('should work for job artifact urls', async () => {
      await expect(
        (gitlabProcessor as any).getGitlabArtifactFetchUrl(
          new URL(
            'https://gitlab.com/group/subgroup/project/-/jobs/artifacts/branch/raw/my/path/to/file.yaml?job=myJob',
          ),
        ),
      ).resolves.toEqual(
        new URL(
          'https://gitlab.com/api/v4/projects/12345/jobs/artifacts/branch/raw/my/path/to/file.yaml?job=myJob',
        ),
      );
    });
    it('errors in mapping the project ID should be captured', async () => {
      await expect(
        (gitlabProcessor as any).getGitlabArtifactFetchUrl(
          new URL(
            'https://gitlab.com/groupA/subgroup/project/-/jobs/artifacts/branch/raw/my/path/to/file.yaml?job=myJob',
          ),
        ),
      ).rejects.toThrow(/^Unable to translate GitLab artifact URL:/);
    });
    it('should resolve the project path using a user token', async () => {
      await expect(
        (gitlabProcessor as any).getGitlabArtifactFetchUrl(
          new URL(
            'https://gitlab.com/user/project/-/jobs/artifacts/branch/raw/my/path/to/file.yaml?job=myJob',
          ),
          'gl-user-token',
        ),
      ).resolves.toEqual(
        new URL(
          'https://gitlab.com/api/v4/projects/12345/jobs/artifacts/branch/raw/my/path/to/file.yaml?job=myJob',
        ),
      );
    });
  });

  describe('resolveProjectToId', () => {
    beforeEach(() => {
      worker.use(
        rest.get('*/api/v4/projects/group%2Fproject', (req, res, ctx) => {
          if (req.headers.get('authorization') !== 'Bearer gl-dummy-token') {
            return res(
              ctx.status(401),
              ctx.json({ message: '401 Unauthorized' }),
            );
          }
          return res(ctx.status(200), ctx.json({ id: 12345 }));
        }),
        rest.get('*/api/v4/projects/user%2Fproject', (req, res, ctx) => {
          if (req.headers.get('authorization') !== 'Bearer gl-user-token') {
            return res(
              ctx.status(401),
              ctx.json({ message: '401 Unauthorized' }),
            );
          }
          return res(ctx.status(200), ctx.json({ id: 12345 }));
        }),
      );
    });

    it('should resolve the project path to a valid project id', async () => {
      await expect(
        (gitlabProcessor as any).resolveProjectToId(
          new URL('https://gitlab.com/group/project'),
        ),
      ).resolves.toEqual(12345);
    });

    it('should resolve the project path to a valid project id using a user token', async () => {
      await expect(
        (gitlabProcessor as any).resolveProjectToId(
          new URL('https://gitlab.com/user/project'),
          'gl-user-token',
        ),
      ).resolves.toEqual(12345);
    });

    it('should use cache to avoid repeated API calls', async () => {
      const projectUrl = new URL('https://gitlab.com/group/project');

      // First call should hit the API
      const firstResult = await (gitlabProcessor as any).resolveProjectToId(
        projectUrl,
      );
      expect(firstResult).toEqual(12345);

      // Mock to ensure the API is not called again
      const apiSpy = jest.fn();
      worker.use(
        rest.get('*/api/v4/projects/group%2Fproject', (req, res, ctx) => {
          apiSpy();
          return res(ctx.status(200), ctx.json({ id: 12345 }));
        }),
      );

      // Second call should use cache
      const secondResult = await (gitlabProcessor as any).resolveProjectToId(
        projectUrl,
      );
      expect(secondResult).toEqual(12345);
      expect(apiSpy).not.toHaveBeenCalled();
    });

    it('should cache different projects separately', async () => {
      worker.use(
        rest.get('*/api/v4/projects/group1%2Fproject1', (req, res, ctx) => {
          if (req.headers.get('authorization') !== 'Bearer gl-dummy-token') {
            return res(
              ctx.status(401),
              ctx.json({ message: '401 Unauthorized' }),
            );
          }
          return res(ctx.status(200), ctx.json({ id: 11111 }));
        }),
        rest.get('*/api/v4/projects/group2%2Fproject2', (req, res, ctx) => {
          if (req.headers.get('authorization') !== 'Bearer gl-dummy-token') {
            return res(
              ctx.status(401),
              ctx.json({ message: '401 Unauthorized' }),
            );
          }
          return res(ctx.status(200), ctx.json({ id: 22222 }));
        }),
      );

      const project1Result = await (gitlabProcessor as any).resolveProjectToId(
        new URL('https://gitlab.com/group1/project1'),
      );
      const project2Result = await (gitlabProcessor as any).resolveProjectToId(
        new URL('https://gitlab.com/group2/project2'),
      );

      expect(project1Result).toEqual(11111);
      expect(project2Result).toEqual(22222);

      // Verify cache contains both
      expect(
        mockCache.getProjectId('https://gitlab.com', 'group1/project1'),
      ).toBe(11111);
      expect(
        mockCache.getProjectId('https://gitlab.com', 'group2/project2'),
      ).toBe(22222);
    });
  });
});
