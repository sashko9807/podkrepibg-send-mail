import bent, { BentResponse } from "bent";

import debugMessage from "debug";

import * as zlib from "zlib";

const debug = debugMessage("sendgrid");

const SENDGRID_API_BASE = "https://api.sendgrid.com/v3";
const SENDGRID_CONTACTS_EXPORT_ENDPOINT = `${SENDGRID_API_BASE}/marketing/contacts/exports`;
const SENGRID_CONTACTS_COUNT = `${SENDGRID_API_BASE}/marketing/contacts/count`;

const asyncSleep = (timeout: number) =>
  new Promise((resolve) => setTimeout(resolve, timeout));

export interface ContactsResponse {
  contact_id: string;
  created_at: string;
  custom_fields: object;
  email: string;
  updated_at: string;
}

interface SendGridExportMetadata {
  prev: string;
  self: string;
  next: string;
  count: number;
}

type SendGridExportResponse = {
  id: string;
  _metadata: SendGridExportMetadata;
};

type SendGridExportStatusResponse = {
  id: string;
  status: "pending" | "failure" | "ready";
  created_at: string;
  updated_at: string;
  completed_at: string;
  expires_at: string;
  urls: string[];
  message?: string;
  _metadata: SendGridExportMetadata;
  contact_count: number;
};

export interface SendgridExportParams {
  list_ids: string[];
  file_type: "json" | "csv";
  segments?: string[];
  max_file_size?: number;
}

interface ISendgrid {
  _apiMethods: ApiMethods;
  getContacts: (opts: SendgridExportParams) => Promise<ContactsResponse[]>;
}

interface SendgridContactCountResponse {
  contact_count: number;
  billable_count: number;
  billable_breakdown: {
    total: number;
    breakdown: object;
  };
}

interface ApiMethods {
  doExportContacts: bent.RequestFunction<SendGridExportResponse>;
  checkExportStatus: bent.RequestFunction<SendGridExportStatusResponse>;
  contactsCount: bent.RequestFunction<SendgridContactCountResponse>;
  get: bent.RequestFunction<BentResponse>;
}

const asyncGunzip = async (buffer: ArrayBuffer) =>
  new Promise<Buffer>((resolve, reject) => {
    zlib.gunzip(buffer, {}, (error, gzipped) => {
      if (error) reject(error);
      resolve(gzipped);
    });
  });

export default class Sendgrid implements ISendgrid {
  _apiMethods: ApiMethods;
  constructor(apiKey: string) {
    const reqHeader = {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    };

    this._apiMethods = {
      doExportContacts: bent<SendGridExportResponse>(
        "POST",
        SENDGRID_CONTACTS_EXPORT_ENDPOINT,
        "json",
        202,
        reqHeader
      ),
      checkExportStatus: bent<SendGridExportStatusResponse>(
        "GET",
        SENDGRID_CONTACTS_EXPORT_ENDPOINT,
        "json",
        200,
        reqHeader
      ),
      // NOTE: We need a raw, non-authorized GET to download the data files.
      // SendGrid's documentation seems inconsistent with the way it works for
      // downloading bulk export job results. Using an Authorization header
      // results in a 405 response code when trying to download export results.
      get: bent<BentResponse>("GET", 200),
      contactsCount: bent<SendgridContactCountResponse>(
        "GET",
        SENGRID_CONTACTS_COUNT,
        "json",
        200,
        reqHeader
      ),
    };
  }

  async getContacts(opts: SendgridExportParams): Promise<ContactsResponse[]> {
    debug(
      `POST ${SENDGRID_CONTACTS_EXPORT_ENDPOINT} with body=${JSON.stringify(
        opts
      )}`
    );
    const responseJSON = await this._apiMethods.doExportContacts("", opts);
    debug(`... response: ${JSON.stringify(responseJSON)}`);

    const jobId = responseJSON.id;
    let exportResponse = await this._apiMethods.checkExportStatus(`/${jobId}`);
    do {
      debug("waiting for sendgrid to finish export...");
      await asyncSleep(10000); // 10s seems to give sendgrid enough time to finish
      exportResponse = await this._apiMethods.checkExportStatus(`/${jobId}`);
      debug(`Export status is ${exportResponse.status}`);
      debug(`GET ${SENDGRID_CONTACTS_EXPORT_ENDPOINT}/${jobId}`);
      debug(`... response: ${JSON.stringify(responseJSON)}`);

      switch (exportResponse.status) {
        case "failure":
          return Promise.reject(exportResponse.message);
        case "ready":
          break;
        default:
      }
    } while (exportResponse.status === "pending");

    const contactsJSON = [] as ContactsResponse[];
    for (const url of exportResponse.urls) {
      debug(`GET ${url}`);
      const response = await this._apiMethods.get(url);
      const gzipped = await response.arrayBuffer();
      const gunzipped = await asyncGunzip(gzipped);
      debug(`... response: ${JSON.stringify(gunzipped)}`);
      // NOTE: The json file from SendGrid isn't valid JSON... work around that.
      // We trim off the final '\n', then split records delimited by '\n'
      // (not ','), then parse each record into a JSON object.
      const records = gunzipped
        .toString()
        .trim()
        .split("\n")
        .map<ContactsResponse>((s: string) => JSON.parse(s));

      contactsJSON.push(...records);
    }

    debug(contactsJSON);
    return contactsJSON;
  }
  async getContactsCount() {
    return await this._apiMethods.contactsCount("");
  }
}
