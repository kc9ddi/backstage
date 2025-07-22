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

import { rest } from 'msw';
import { setupServer } from 'msw/node';
import { GitLabIntegrationConfig } from './config';
import { getGitLabFileFetchUrl, getGitLabRequestOptions } from './core';
import { GitlabProjectIdMapCache } from './types';

const worker = setupServer();

describe('gitlab core', () => {
  beforeAll(() => worker.listen({ onUnhandledRequest: 'error' }));
  afterAll(() => worker.close());
  afterEach(() => worker.resetHandlers());

  let mockCache: jest.Mocked<GitlabProjectIdMapCache>;

  beforeEach(() => {
    mockCache = {
      getProjectId: jest.fn(),
      setProjectId: jest.fn(),
    };

    worker.use(
      rest.get('*/api/v4/projects/group%2Fproject', (_, res, ctx) =>
        res(ctx.status(200), ctx.json({ id: 12345 })),
      ),
      rest.get('*/api/v4/projects/group%2Fsubgroup%2Fproject', (_, res, ctx) =>
        res(ctx.status(200), ctx.json({ id: 12345 })),
      ),
    );
  });

  const configWithNoToken: GitLabIntegrationConfig = {
    host: 'gitlab.com',
    apiBaseUrl: '<ignored>',
    baseUrl: '<ignored>',
  };

  const configSelfHosteWithRelativePath: GitLabIntegrationConfig = {
    host: 'gitlab.mycompany.com',
    token: '0123456789',
    apiBaseUrl: '<ignored>',
    baseUrl: 'https://gitlab.mycompany.com/gitlab',
  };

  const configSelfHostedWithoutRelativePath: GitLabIntegrationConfig = {
    host: 'gitlab.mycompany.com',
    token: '0123456789',
    apiBaseUrl: '<ignored>',
    baseUrl: 'https://gitlab.mycompany.com',
  };

  describe('getGitLabFileFetchUrl', () => {
    describe('when target has a scoped route', () => {
      it('returns a projects API URL', async () => {
        const target =
          'https://gitlab.com/group/project/-/blob/branch/folder/file.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/12345/repository/files/folder%2Ffile.yaml/raw?ref=branch';
        await expect(
          getGitLabFileFetchUrl(target, configWithNoToken),
        ).resolves.toBe(fetchUrl);
      });

      it('supports folder named "blob"', async () => {
        const target =
          'https://gitlab.com/group/project/-/blob/branch/blob/file.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/12345/repository/files/blob%2Ffile.yaml/raw?ref=branch';
        await expect(
          getGitLabFileFetchUrl(target, configWithNoToken),
        ).resolves.toBe(fetchUrl);
      });

      it('locates projects in subgroups', async () => {
        const target =
          'https://gitlab.com/group/subgroup/project/-/blob/branch/folder/file.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/12345/repository/files/folder%2Ffile.yaml/raw?ref=branch';
        await expect(
          getGitLabFileFetchUrl(target, configWithNoToken),
        ).resolves.toBe(fetchUrl);
      });

      it('supports filename with .yml extension', async () => {
        const target =
          'https://gitlab.com/group/project/-/blob/branch/folder/file.yml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/12345/repository/files/folder%2Ffile.yml/raw?ref=branch';
        await expect(
          getGitLabFileFetchUrl(target, configWithNoToken),
        ).resolves.toBe(fetchUrl);
      });

      it('supports non-URI-encoded target', async () => {
        const target =
          'https://gitlab.com/group/project/-/blob/branch/folder/file with spaces.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/12345/repository/files/folder%2Ffile%20with%20spaces.yaml/raw?ref=branch';
        await expect(
          getGitLabFileFetchUrl(target, configWithNoToken),
        ).resolves.toBe(fetchUrl);
      });

      describe('when gitlab is self-hosted', () => {
        it('returns projects API URL', async () => {
          const target =
            'https://gitlab.mycompany.com/group/project/-/blob/branch/folder/file.yaml';
          const fetchUrl =
            'https://gitlab.mycompany.com/api/v4/projects/12345/repository/files/folder%2Ffile.yaml/raw?ref=branch';
          await expect(
            getGitLabFileFetchUrl(target, configSelfHostedWithoutRelativePath),
          ).resolves.toBe(fetchUrl);
        });

        it('handles non-URI-encoded target', async () => {
          const target =
            'https://gitlab.mycompany.com/group/project/-/blob/branch/folder/file with spaces.yaml';
          const fetchUrl =
            'https://gitlab.mycompany.com/api/v4/projects/12345/repository/files/folder%2Ffile%20with%20spaces.yaml/raw?ref=branch';
          await expect(
            getGitLabFileFetchUrl(target, configSelfHostedWithoutRelativePath),
          ).resolves.toBe(fetchUrl);
        });

        describe('with a relative path', () => {
          it('returns projects API URL', async () => {
            const target =
              'https://gitlab.mycompany.com/gitlab/group/project/-/blob/branch/folder/file.yaml';
            const fetchUrl =
              'https://gitlab.mycompany.com/gitlab/api/v4/projects/12345/repository/files/folder%2Ffile.yaml/raw?ref=branch';
            await expect(
              getGitLabFileFetchUrl(target, configSelfHosteWithRelativePath),
            ).resolves.toBe(fetchUrl);
          });

          it('handles non-URI-encoded target', async () => {
            const target =
              'https://gitlab.mycompany.com/gitlab/group/project/-/blob/branch/folder/file with spaces.yaml';
            const fetchUrl =
              'https://gitlab.mycompany.com/gitlab/api/v4/projects/12345/repository/files/folder%2Ffile%20with%20spaces.yaml/raw?ref=branch';
            await expect(
              getGitLabFileFetchUrl(target, configSelfHosteWithRelativePath),
            ).resolves.toBe(fetchUrl);
          });
        });
      });
    });

    describe('when target has an unscoped route', () => {
      it('returns projects API URL', async () => {
        const target =
          'https://gitlab.com/group/project/blob/branch/folder/file.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/12345/repository/files/folder%2Ffile.yaml/raw?ref=branch';
        await expect(
          getGitLabFileFetchUrl(target, configWithNoToken),
        ).resolves.toBe(fetchUrl);
      });

      it('supports project in subgroup', async () => {
        const target =
          'https://gitlab.com/group/subgroup/project/blob/branch/folder/file.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/12345/repository/files/folder%2Ffile.yaml/raw?ref=branch';
        await expect(
          getGitLabFileFetchUrl(target, configWithNoToken),
        ).resolves.toBe(fetchUrl);
      });

      it('supports repo with branch named "blob"', async () => {
        const target =
          'https://gitlab.com/group/project/blob/blob/folder/file.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/12345/repository/files/folder%2Ffile.yaml/raw?ref=blob';
        await expect(
          getGitLabFileFetchUrl(target, configWithNoToken),
        ).resolves.toBe(fetchUrl);
      });
    });

    describe('with caching behavior', () => {
      it('uses cached project ID when available', async () => {
        const target =
          'https://gitlab.com/group/project/-/blob/branch/folder/file.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/67890/repository/files/folder%2Ffile.yaml/raw?ref=branch';

        // Mock cache to return a cached project ID
        mockCache.getProjectId.mockReturnValue(67890);

        const result = await getGitLabFileFetchUrl(
          target,
          configWithNoToken,
          undefined,
          mockCache,
        );

        expect(result).toBe(fetchUrl);
        expect(mockCache.getProjectId).toHaveBeenCalledWith(
          'https://gitlab.com-group/project',
          'group/project',
        );
        expect(mockCache.setProjectId).not.toHaveBeenCalled();
      });

      it('fetches and caches project ID when not in cache', async () => {
        const target =
          'https://gitlab.com/group/project/-/blob/branch/folder/file.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/12345/repository/files/folder%2Ffile.yaml/raw?ref=branch';

        // Mock cache to return undefined (cache miss)
        mockCache.getProjectId.mockReturnValue(undefined);

        const result = await getGitLabFileFetchUrl(
          target,
          configWithNoToken,
          undefined,
          mockCache,
        );

        expect(result).toBe(fetchUrl);
        expect(mockCache.getProjectId).toHaveBeenCalledWith(
          'https://gitlab.com-group/project',
          'group/project',
        );
        expect(mockCache.setProjectId).toHaveBeenCalledWith(
          'https://gitlab.com-group/project',
          'group/project',
          12345,
        );
      });

      it('works with self-hosted GitLab with relative path caching', async () => {
        const target =
          'https://gitlab.mycompany.com/gitlab/group/project/-/blob/branch/folder/file.yaml';
        const fetchUrl =
          'https://gitlab.mycompany.com/gitlab/api/v4/projects/54321/repository/files/folder%2Ffile.yaml/raw?ref=branch';

        // Mock cache to return a cached project ID
        mockCache.getProjectId.mockReturnValue(54321);

        const result = await getGitLabFileFetchUrl(
          target,
          configSelfHosteWithRelativePath,
          undefined,
          mockCache,
        );

        expect(result).toBe(fetchUrl);
        expect(mockCache.getProjectId).toHaveBeenCalledWith(
          'https://gitlab.mycompany.com/gitlab-group/project',
          'group/project',
        );
        expect(mockCache.setProjectId).not.toHaveBeenCalled();
      });

      it('works with subgroups and caching', async () => {
        const target =
          'https://gitlab.com/group/subgroup/project/-/blob/branch/folder/file.yaml';
        const fetchUrl =
          'https://gitlab.com/api/v4/projects/98765/repository/files/folder%2Ffile.yaml/raw?ref=branch';

        // Mock cache to return a cached project ID
        mockCache.getProjectId.mockReturnValue(98765);

        const result = await getGitLabFileFetchUrl(
          target,
          configWithNoToken,
          undefined,
          mockCache,
        );

        expect(result).toBe(fetchUrl);
        expect(mockCache.getProjectId).toHaveBeenCalledWith(
          'https://gitlab.com-group/subgroup/project',
          'group/subgroup/project',
        );
        expect(mockCache.setProjectId).not.toHaveBeenCalled();
      });
    });
  });

  describe('getGitLabRequestOptions', () => {
    it('should return Authorization bearer header when a token is provided', () => {
      const token = '1234567890';
      const result = getGitLabRequestOptions(
        configSelfHosteWithRelativePath,
        token,
      );

      expect(result).toEqual({
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
    });

    it('should return Authorization bearer header using the config token when no token is provided', () => {
      const result = getGitLabRequestOptions(configSelfHosteWithRelativePath);

      expect(result).toEqual({
        headers: {
          Authorization: `Bearer ${configSelfHosteWithRelativePath.token}`,
        },
      });
    });
  });
});
