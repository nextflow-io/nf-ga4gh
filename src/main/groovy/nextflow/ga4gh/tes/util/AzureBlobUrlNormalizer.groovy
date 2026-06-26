/*
 * Copyright (c) Nextflow Contributors
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

package nextflow.ga4gh.tes.util

/**
 * Normalizes Azure Blob Storage URLs to a canonical format.
 * Supports multiple URL formats used across TES clients and orchestrators:
 * - Path-style: /storageAccount/container/blob/path
 * - Azure SDK style: https://storageAccount.blob.core.windows.net/container/blob/path
 * - Nextflow/orchestrator style: az://storageAccount/container/blob/path
 * 
 * Canonical output format: az://storageAccount/container/blob/path
 */
class AzureBlobUrlNormalizer {

    /**
     * Normalizes a URL to the canonical az:// format.
     *
     * @param url The URL to normalize (can be path-style, https, or az://)
     * @return Normalized URL in az://storageAccount/container/blob/path format, or original URL if not an Azure path
     */
    static String normalizeToAzurePath(String url) {
        if (!url?.trim()) {
            return url
        }

        // Already in canonical az:// format
        if (url.startsWith('az://')) {
            return url.replaceAll('/+$', '')
        }

        // Path-style: /storageAccount/container/blob/path
        if (url.startsWith('/')) {
            def segments = url.split('/').findAll { it }  // remove empty segments
            if (segments.size() >= 2) {
                def storageAccount = segments[0]
                def container = segments[1]
                def blobPath = segments.drop(2).join('/')
                def suffix = blobPath ? "/${blobPath}" : ''
                return "az://${storageAccount}/${container}${suffix}".replaceAll('/+$', '')
            }
        }

        // HTTPS blob format: https://storageAccount.blob.core.windows.net/container/blob/path
        if (url.startsWith('https://') && url.contains('.blob.core.windows.net/')) {
            try {
                def uri = url.toURI()
                def host = uri.host ?: ''
                def storageAccount = host.tokenize('.')[0]
                def pathSegments = uri.path.split('/').findAll { it }
                
                if (storageAccount && pathSegments.size() >= 1) {
                    def container = pathSegments[0]
                    def blobPath = pathSegments.drop(1).join('/')
                    def suffix = blobPath ? "/${blobPath}" : ''
                    return "az://${storageAccount}/${container}${suffix}".replaceAll('/+$', '')
                }
            } catch (Exception e) {
                // Return original if URI parsing fails
                return url
            }
        }

        // Not an Azure path format - return as-is (could be local path, http://, etc)
        return url
    }

    /**
     * Converts an az:// URL to HTTPS blob format for Azure SDK clients.
     *
     * @param storageAccountName Azure storage account name
     * @param url The az:// URL
     * @return HTTPS blob URL in format https://account.blob.core.windows.net/container/blob/path
     */
    static String toHttpsBlobUrl(String storageAccountName, String url) {
        if (!url?.trim()) {
            return url
        }

        // Normalize first
        def normalized = normalizeToAzurePath(url)

        if (!normalized.startsWith('az://')) {
            // Not an Azure path, return original
            return url
        }

        // Extract path from az://storageAccount/container/blob/path
        def pathPart = normalized.substring('az://'.length())
        def segments = pathPart.split('/').findAll { it }
        if (segments.size() >= 3 && storageAccountName && segments[0] == storageAccountName) {
            def normalizedAccount = segments[0]
            def containerAndBlob = segments.drop(1).join('/')
            return "https://${normalizedAccount}.blob.core.windows.net/${containerAndBlob}"
        }

        // Backward compatibility for legacy az://container/blob/path inputs,
        // and for ambiguous az:// paths where the account segment is missing.
        return "https://${storageAccountName}.blob.core.windows.net/${pathPart}"
    }

    /**
     * Validates that a URL is in a supported Azure format.
     *
     * @param url The URL to validate
     * @return True if URL is in a recognized Azure format
     */
    static boolean isAzureStorageUrl(String url) {
        if (!url?.trim()) {
            return false
        }

        return url.startsWith('az://') ||
               url.startsWith('/') ||
               (url.startsWith('https://') && url.contains('.blob.core.windows.net/'))
    }
}
