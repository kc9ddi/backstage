/*
 * Copyright 2023 The Backstage Authors
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

/**
 * @public
 */
export type GitlabCredentials = {
  headers?: { [name: string]: string };
  token?: string;
};

/**
 * @public
 */
export interface GitlabCredentialsProvider {
  getCredentials(opts: { url: string }): Promise<GitlabCredentials>;
}

/**
 * A cache for GitLab project IDs.
 *
 * @public
 */
export interface GitlabProjectIdMapCache {
  getProjectId(projectPath: string, repository: string): number | undefined;
  setProjectId(
    projectPath: string,
    repository: string,
    projectId: number,
  ): void;
}
