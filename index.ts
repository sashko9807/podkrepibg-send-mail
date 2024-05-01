import { config } from "dotenv";
import { PrismaClient } from "@prisma/client";
import { MailService, MailDataRequired } from "@sendgrid/mail";
import SendGrid, { ContactsResponse } from "./lib/sendgrid";
import { createHash, randomUUID } from "crypto";
import createDebugMessage from "debug";
import { PersonalizationData } from "@sendgrid/helpers/classes/personalization";
import { SendgridExportParams } from "./lib/sendgrid";

const sgMail = new MailService();
sgMail.setApiKey(process.env.SENDGRID_API_KEY as string);
const sg = new SendGrid(process.env.SENDGRID_API_KEY as string);
config();

const debug = createDebugMessage("index");
debug.enabled;

type Object = {
  id: string;
  hash: string;
  registered: boolean;
};

type UnregisteredInsert = {
  id: string;
  email: string;
  consent: boolean;
};

const prisma = new PrismaClient();

function generateHash(record_id: string) {
  const hash = createHash("sha256").update(record_id).digest("hex");
  return hash;
}

async function main() {
  const sendList: Map<string, Object> = new Map();
  const opts: SendgridExportParams = {
    list_ids: [process.env.SENDGRID_LIST_ID as string],
    file_type: "json",
  };
  // const contacts: ContactsResponse[] = [
  //   {
  //     contact_id: "7e7d15f8-1c3f-4cd8-ad9c-50b8e32eab44",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "sashko506@gmail.com",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  // ];
  const contacts = await sg.getContacts(opts);

  // const contacts = [
  //   {
  //     contact_id: "7e7d15f8-1c3f-4cd8-ad9c-50b8e32eab44",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "admin@podkrepi.bg",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "247241b4-5129-410f-bea8-001c70833334",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "realhillary@hotmail.fr",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "a414b310-2280-45b1-8298-1f0b0364a178",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "tiredjake76@yahoo.com",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "a0849ed3-4f8c-448b-9b63-c3e4948c15dd",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "melodycurious@t-online.de",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "de6883d8-f2e7-4d34-af8b-bd8358f35c57",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "goodheidi@free.fr",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "10e495a7-7316-4245-9f67-7b4a1ce43cd1",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "wickedleonard@yahoo.es",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "fd517dd4-5194-40dd-ac8c-830088c2672d",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "hilarypuzzled@aol.com",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "f187facb-f343-4291-aa0e-b505d2a1e50c",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "agreeableshawna@yahoo.co.jp",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "5c65da0c-d617-4dd3-b895-3de72f2610c7",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "gentleandre80@tiscali.it",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "1bdc70a2-43d1-4da3-8c93-eb8a1a7a653f",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "calmnancy76@gmx.de",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "0f3efc62-8ae8-4f1a-9ffb-6932ad2db92a",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "barrysleepy@sfr.fr",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "74cdc50a-aaa2-4416-901e-451da84b4a97",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "jodipuzzled@yahoo.com.ar",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "777ee0ec-1ff6-4863-b263-5748d4c763de",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "selfishkyle56@home.nl",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "95c9edd5-6cce-40d4-b500-a40a8982516d",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "keriterrible@hotmail.fr",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "0c4459e6-d21e-4571-a9b8-8bec16299e27",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "agreeablebrett@me.com",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "645bb89c-7553-4b13-a595-ffe402314fc8",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "friendlytodd@home.nl",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "a2dae48d-5f23-4c7e-8ee1-79c081af8903",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "darryldisturbed@live.com.au",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  //   {
  //     contact_id: "1e14347e-00d5-4c88-b440-dd77f3f0dcc2",
  //     created_at: "2024-03-17T13:07:41Z",
  //     custom_fields: {},
  //     email: "jaimerich@bigpond.com",
  //     updated_at: "2024-03-17T13:07:42Z",
  //   },
  // ];
  debug(`Length of contacts` + contacts.length);
  debug("Generating Hashmap of emails");
  const emailList = contacts.map((contact) => contact.email);
  for (const email of emailList) {
    const id = randomUUID();
    sendList.set(email, {
      id: id,
      hash: generateHash(id),
      registered: false,
    });
  }

  const registeredMails = await prisma.person.findMany({
    where: { email: { in: emailList } },
  });

  const unregisteredUsers =
    await prisma.unregisteredNotificationConsent.findMany();

  //Create the initial set of emails

  //Date of when change has been deployed
  //https://github.com/podkrepi-bg/frontend/releases/tag/v1.7.0

  const dateOfDeploy = new Date("2023-08-23");
  debug("Removing emails registered after 23th of August 2023");
  for (const registeredUser of registeredMails) {
    const createdAt = new Date(registeredUser.createdAt);

    // Remove email if it belongs to user created after the change has been deployed, as they had already decided
    // whether to give consent or not.
    if (
      sendList.get(registeredUser.email as string) &&
      createdAt > dateOfDeploy
    ) {
      sendList.delete(registeredUser.email as string);
      continue;
    }
    //Update the value of this mail
    sendList.set(registeredUser.email as string, {
      ...sendList.get(registeredUser.email as string),
      id: registeredUser.id,
      hash: generateHash(registeredUser.id),
      registered: true,
    });
  }

  debug("Removing emails in unregistered consent emails");
  for (const consent of unregisteredUsers) {
    if (sendList.has(consent.email)) {
      sendList.delete(consent.email);
      continue;
    }
  }

  const CHUNK_SIZE = process.env.CHUNK_SIZE as unknown as number;

  debug(`Splitting sendList into chunks of ${CHUNK_SIZE} elements`);
  const emailListChunked = Array.from(sendList.entries()).reduce<
    Map<string, Object>[]
  >((chunk, curr, i) => {
    const ch = Math.floor(i / CHUNK_SIZE);
    if (!chunk[ch]) {
      chunk[ch] = new Map();
    }

    chunk[ch].set(curr[0], curr[1]);
    return chunk;
  }, []);

  //Get emails that are not registered, and add them to unregistered consent
  const emailsToAdd: UnregisteredInsert[] = [];
  for (const [key, value] of sendList) {
    if (value.registered) continue;
    emailsToAdd.push({ id: value.id, email: key, consent: false });
  }

  await prisma.unregisteredNotificationConsent.createMany({
    data: emailsToAdd,
  });

  const TIMEOUT = 6 * 10 * 1000;
  // Send emails on batch. 1000 emails per EMAIL_TIMEOUT

  emailListChunked.forEach((value, index) => {
    (function (value: Map<string, Object>) {
      setTimeout(() => {
        const personalMail = [...value.entries()].map<PersonalizationData>(
          ([key, value]: [string, Object]) => {
            return {
              to: { email: key, name: "" },
              dynamicTemplateData: {
                subscribe_link: `${process.env.APP_URL}/notifications/subscribe?hash=${value.hash}&email=${key}&consent=true`,
                unsubscribe_link: `${process.env.APP_URL}/notifications/unsubscribe?email=${key}`,
                subject: "Абониране за известия - Тест",
              },
            };
          }
        );
        const message: MailDataRequired = {
          personalizations: personalMail,
          from: "admin@beaconms.ovh",
          content: [{ type: "text/html", value: "Subscription Notification" }],
          templateId: process.env.SENDGRID_TEMPLATE_ID,
        };
        sgMail.send(message);
        debug(`Sending batch ${index + 1}`);
      }, TIMEOUT * index);
    })(value);
  });

  return contacts;
}

main()
  .then(async () => await prisma.$disconnect())
  .catch(async (e) => {
    console.error(e);
    prisma.$disconnect();
  });

// async function contactsCount() {
//   const res = await sg.getContactsCount();
//   console.log(res);
// }

// contactsCount();
