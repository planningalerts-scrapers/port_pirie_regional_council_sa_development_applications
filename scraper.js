// Parses the development application at the South Australian Port Pirie Regional Council site
// and places them in a database.
//
// Michael Bone
// 3rd August 2018

"use strict";

let cheerio = require("cheerio");
let request = require("request-promise-native");
let sqlite3 = require("sqlite3").verbose();
let pdf2json = require("pdf2json");
let urlparser = require("url");
let moment = require("moment");
let fs = require("fs");

const DevelopmentApplicationsUrl = "http://www.pirie.sa.gov.au/page.aspx?u=646#.W2REvfZuKUl";
const CommentUrl = "mailto:council@pirie.sa.gov.au";

// Address information.

let StreetAndSuburbNames = null;
let StreetSuffixes = null;
let SuburbNames = null;
let HundredSuburbNames = null;

// Sets up an sqlite database.

async function initializeDatabase() {
    return new Promise((resolve, reject) => {
        let database = new sqlite3.Database("data.sqlite");
        database.serialize(() => {
            database.run("create table if not exists [data] ([council_reference] text primary key, [address] text, [description] text, [info_url] text, [comment_url] text, [date_scraped] text, [date_received] text, [on_notice_from] text, [on_notice_to] text)");
            resolve(database);
        });
    });
}

// Inserts a row in the database if it does not already exist.

async function insertRow(database, developmentApplication) {
    return new Promise((resolve, reject) => {
        let sqlStatement = database.prepare("insert or ignore into [data] values (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        sqlStatement.run([
            developmentApplication.applicationNumber,
            developmentApplication.address,
            developmentApplication.reason,
            developmentApplication.informationUrl,
            developmentApplication.commentUrl,
            developmentApplication.scrapeDate,
            developmentApplication.receivedDate,
            null,
            null
        ], function(error, row) {
            if (error) {
                console.error(error);
                reject(error);
            } else {
                if (this.changes > 0)
                    console.log(`    Inserted: application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\" and reason \"${developmentApplication.reason}\" into the database.`);
                else
                    console.log(`    Skipped: application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\" and reason \"${developmentApplication.reason}\" because it was already present in the database.`);
                sqlStatement.finalize();  // releases any locks
                resolve(row);
            }
        });
    });
}

// Format the address, ensuring that it has a valid suburb, state and post code.

function formatAddress(houseNumber, streetName, suburbName, hundredName) {
    houseNumber = houseNumber.trim().replace(/\s\s+/g, " ");  // replace multiple whitespace characters with a single space
    streetName = streetName.trim().replace(/\s\s+/g, " ");  // replace multiple whitespace characters with a single space
    suburbName = suburbName.trim().replace(/\s\s+/g, " ");  // replace multiple whitespace characters with a single space
    hundredName = hundredName.trim().replace(/\s\s+/g, " ");  // replace multiple whitespace characters with a single space

    // The address is considered invalid if there is no street name.

    if (streetName === "" || streetName === "0")
        return "";  // invalid address

    // If the hundred name appears in the street name then blank out the street name.

    if (/^HD /.test(streetName) || /^HUNDRED /.test(streetName))  // for example, "HD NAPPERBY" or "HUNDRED WANDEARAH"
        streetName = "";

    // Remove the text "(NAP)" that appears in some street names.

    streetName = streetName.replace(/\(NAP\) /g, "");

    // Change "ROA" to "ROAD" that appears as the suffix of some street names.

    streetName = streetName.replace(/ ROA$/g, " ROAD");

    // Attempt to expand any street suffix abbreviations.  For example, "St" becomes "Street".
    // Also remove any duplicated items that appear in the street, such as "STREET STREET".

    let streetNameTokens = streetName.split(" ");

    if (streetNameTokens.length >= 2 && streetNameTokens[0] === streetNameTokens[1])
        streetNameTokens.shift();
    if (streetNameTokens.length >= 2 && streetNameTokens[streetNameTokens.length - 2] === streetNameTokens[streetNameTokens.length - 1])
        streetNameTokens.pop();

    let streetSuffixAbbreviation = streetNameTokens[streetNameTokens.length - 1] || "";
    let streetSuffix = StreetSuffixes[streetSuffixAbbreviation.toLowerCase()];
    if (streetSuffix !== undefined)
        streetNameTokens[streetNameTokens.length - 1] = (streetSuffixAbbreviation === streetSuffixAbbreviation.toUpperCase()) ? streetSuffix.toUpperCase() : streetSuffix;

    streetName = streetNameTokens.join(" ");

    // Ensure that the suburb exists and that it includes a state and a post code.  For example,
    // "PIRIE SA 5240".

    if (!/ SA [0-9]{4}$/.test(suburbName))
        suburbName = SuburbNames[suburbName.toLowerCase()] || "";

    // If the suburb was not determined then attempt to derive the suburb by using a combination
    // of the suburbs determined based on the street name and the suburbs determined based on the
    // hundred name.

    if (suburbName === "") {
        // Obtain the suburbs associated with the street, if it exists.  Note that there may be
        // more than one suburb because the same street can pass through multiple suburbs and
        // the same street name can be used in non-adjoining suburbs).

        let streetSuburbNames = StreetAndSuburbNames[streetName.toLowerCase()];

        // Obtain the suburbs associated with any hundred name.

        let hundredSuburbNames = HundredSuburbNames[hundredName.replace(/^HD OF /g, "").trim().toLowerCase()];  // converts "HD OF NAPPERBY" to "NAPPERBY"

        if (streetSuburbNames === undefined && hundredSuburbNames === undefined)
            return "";  // a suburb cannot be determined, the address is invalid
        else if (streetSuburbNames !== undefined && streetSuburbNames.length === 1)
            suburbName = streetSuburbNames[0];  // exactly one match (this is the best choice)
        else if (hundredSuburbNames !== undefined && hundredSuburbNames.length === 1)
            suburbName = hundredSuburbNames[0];  // exactly one match (this is the best choice)
        else if (streetSuburbNames !== undefined && hundredSuburbNames === undefined)
            suburbName = streetSuburbNames[0];  // there are multiple suburbs, arbitrarily choose the first
        else if (streetSuburbNames === undefined && hundredSuburbNames !== undefined)
            suburbName = hundredSuburbNames[0];  // there are multiple suburbs, arbitrarily choose the first
        else {
            // Determine the intersection of the two arrays of suburb names and arbitrarily choose
            // the first suburb.  If the intersection is empty then arbitrarily choose the first
            // suburb associated with the street.

            let intersectingSuburbNames = streetSuburbNames.filter(suburbName => hundredSuburbNames.indexOf(suburbName) >= 0);
            suburbName = (intersectingSuburbNames.length > 0) ? intersectingSuburbNames[0] : streetSuburbNames[0];
        }
    }

    // The address is considered invalid if a suburb name could not be determined.

    if (suburbName === "")
        return "";
    else if (houseNumber === "" && streetName === "")
        return suburbName.trim();
    else        
        return ((houseNumber + " " + streetName).trim() + ", " + suburbName).trim();
}

// Reads all the address information into global objects.

function readAddressInformation() {
    StreetAndSuburbNames = {}
    for (let line of fs.readFileSync("streetnames.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let streetNameTokens = line.split(",");
        let streetName = streetNameTokens[0].trim().toLowerCase();
        let suburbName = streetNameTokens[1].trim();
        if (StreetAndSuburbNames[streetName] === undefined)
            StreetAndSuburbNames[streetName] = [];
        StreetAndSuburbNames[streetName].push(suburbName);  // several suburbs may exist for the same street name
    }

    StreetSuffixes = {};
    for (let line of fs.readFileSync("streetsuffixes.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let streetSuffixTokens = line.split(",");
        StreetSuffixes[streetSuffixTokens[0].trim().toLowerCase()] = streetSuffixTokens[1].trim();
    }

    SuburbNames = {};
    HundredSuburbNames = {};
    for (let line of fs.readFileSync("suburbnames.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let suburbTokens = line.split(",");
        let suburbName = suburbTokens[0].trim().toLowerCase();
        let suburbStateAndPostCode = suburbTokens[1].trim();
        SuburbNames[suburbName] = suburbStateAndPostCode;
        for (let hundredName of suburbTokens[2].split(";")) {
            hundredName = hundredName.trim().toLowerCase();
            if (HundredSuburbNames[hundredName] === undefined)
                HundredSuburbNames[hundredName] = [];
            HundredSuburbNames[hundredName].push(suburbStateAndPostCode);  // several suburbs may exist for the same hundred name
        }
    }
}

// Parses the development applications.

async function main() {
    // Ensure that the database exists.

    let database = await initializeDatabase();

    // Read all street, street suffix, suburb, state, post code and hundred information.

    readAddressInformation();

    // Retrieve the page contains the links to the PDFs.

    console.log(`Retrieving page: ${DevelopmentApplicationsUrl}`);
    let body = await request(DevelopmentApplicationsUrl);
    let $ = cheerio.load(body);

    let pdfUrls = [];
    for (let element of $("a[href$='.pdf']").get()) {
        let pdfUrl = new urlparser.URL(element.attribs.href, DevelopmentApplicationsUrl).href;
        if (!pdfUrls.some(url => url === pdfUrl))
            pdfUrls.push(pdfUrl);
    }

    if (pdfUrls.length === 0) {
        console.log("No PDF URLs were found on the page.");
        return;
    }

    // Select the most recent PDF.  And randomly select one other PDF (avoid processing all PDFs
    // at once because this may use too much memory, resulting in morph.io terminating the current
    // process).

    let selectedPdfUrls = [];
    selectedPdfUrls.push(pdfUrls.shift());
    if (pdfUrls.length > 0)
        selectedPdfUrls.push(pdfUrls[getRandom(1, pdfUrls.length)]);

    for (let pdfUrl of selectedPdfUrls) {
        console.log(`Retrieving document: ${pdfUrl}`);

        // Parse the PDF into a collection of PDF rows.  Each PDF row is simply an array of
        // strings, being the text that has been parsed from the PDF.

        let pdfParser = new pdf2json();
        let pdfPipe = request({ url: pdfUrl, encoding: null }).pipe(pdfParser);
        pdfPipe.on("pdfParser_dataError", error => {
            console.log("In pdfParser_dataError catch.");
            console.log(error);
        });
        pdfPipe.on("pdfParser_dataReady", async pdf => {
            try {
                // Convert the JSON representation of the PDF into a collection of PDF rows.

                console.log(`Parsing document: ${pdfUrl}`);
                let rows = convertPdfToText(pdf);

                let developmentApplications = [];
                let developmentApplication = null;
                let isReason = false;

                for (let row of rows) {
                    let text = (row.length === 0) ? "" : row[0].trim().toLowerCase();
                    if (text.startsWith("application no")) {
                        developmentApplication = {
                            applicationNumber: row[1].trim(),
                            address: "",
                            reason: "",
                            informationUrl : pdfUrl,
                            commentUrl: CommentUrl,
                            scrapeDate : moment().format("YYYY-MM-DD"),
                            receivedDate: "",
                            houseNumber: "",
                            streetName: "",
                            suburbName: "",
                            hundredName: ""
                        }
                        developmentApplications.push(developmentApplication);
                        isReason = false;
                        for (let index = 2; index < row.length; index++) {
                            let receivedDate = moment(row[index].trim(), "D/MM/YYYY", true);  // allows the leading zero of the day to be omitted
                            if (receivedDate.isValid()) {
                                developmentApplication.receivedDate = receivedDate.format("YYYY-MM-DD");
                                break;
                            }
                        }
                    } else if (developmentApplication !== null) {
                        if (text.startsWith("property house no") && row.length >= 2 && row[1].trim() !== "0" && row[1].trim().toLowerCase() !== "building conditions") {
                            developmentApplication.houseNumber = row[1].replace(/\+ü/g, " ").replace(/ü/g, " ").trim();
                        } else if (text.startsWith("property street") && row.length >= 2 && row[1].trim() !== "0" && row[1].replace(/ü/g, " ").toUpperCase() === row[1].replace(/ü/g, " ")) {
                            developmentApplication.streetName = row[1].replace(/\+ü/g, " ").replace(/ü/g, " ").trim();
                        } else if (text.startsWith("property suburb") && row.length >= 2 && row[1].trim() !== "0" && row[1].trim().toLowerCase() != "lodgement fee - base amount" && row[1] === row[1].toUpperCase()) {
                            developmentApplication.suburbName = row[1].trim();
                        } else if (text.startsWith("hundred") && row.length >= 2 && row[1].trim() !== "0" && !row[1].trim().startsWith("$") && row[1] === row[1].toUpperCase()) {
                            developmentApplication.hundredName = row[1].trim();
                        } else if (text.startsWith("development description")) {
                            isReason = true;
                        } else if (isReason && text.startsWith("private certifier name")) {
                            isReason = false;
                            developmentApplication = null;
                        } else if (isReason && row.length >= 1 && row[0].toUpperCase() === row[0]) {
                            developmentApplication.reason += ((developmentApplication.reason === "") ? "" : " ") + row[0].trim();
                        }
                    }
                }

                for (let developmentApplication of developmentApplications) {
                    developmentApplication.address = formatAddress(developmentApplication.houseNumber, developmentApplication.streetName, developmentApplication.suburbName, developmentApplication.hundredName).trim().replace(/\s\s+/g, " ");
                    if (developmentApplication.reason.trim() === "")
                        developmentApplication.reason = "No description provided";
                    if (developmentApplication.applicationNumber.trim() !== "" && developmentApplication.address.trim() !== "")
                        await insertRow(database, developmentApplication);
                }

                console.log(`Parsed document: ${pdfUrl}`);

                // Attempt to avoid reaching 512 MB memory usage (this will otherwise result in
                // the current process being terminated by morph.io).

                if (global.gc)
                    global.gc();
            } catch (ex) {
                console.log("In pdfParser_dataReady catch.");
                console.log(ex);
            }
        });
    }
}

// Gets a random integer in the specified range: [minimum, maximum).

function getRandom(minimum, maximum) {
    return Math.floor(Math.random() * (Math.floor(maximum) - Math.ceil(minimum))) + Math.ceil(minimum);
}

// Convert a parsed PDF into an array of rows.  This function is based on pdf2table by Sam Decrock.
// See https://github.com/SamDecrock/pdf2table/blob/master/lib/pdf2table.js.
//
// Copyright (c) 2015 Sam Decrock <sam.decrock@gmail.com>
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

function convertPdfToText(pdf) {
    let xComparer = (a, b) => (a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0);
    let yComparer = (a, b) => (a.y > b.y) ? 1 : ((a.y < b.y) ? -1 : 0);

    // Find the smallest Y co-ordinate for two texts with equal X co-ordinates.

    let smallestYValueForPage = [];

    for (let pageIndex = 0; pageIndex < pdf.formImage.Pages.length; pageIndex++) {
        let page = pdf.formImage.Pages[pageIndex];
        let smallestYValue = null;  // per page
        let textsWithSameXValues = {};

        for (let textIndex = 0; textIndex < page.Texts.length; textIndex++) {
            let text = page.Texts[textIndex];
            if (!textsWithSameXValues[text.x])
                textsWithSameXValues[text.x] = [];
            textsWithSameXValues[text.x].push(text);
        }

        // Find smallest Y distance.

        for (let x in textsWithSameXValues) {
            let texts = textsWithSameXValues[x];
            for (let i = 0; i < texts.length; i++) {
                for (let j = 0; j < texts.length; j++) {
                    if (texts[i] !== texts[j]) {
                        let distance = Math.abs(texts[j].y - texts[i].y);
                        if (smallestYValue === null || distance < smallestYValue)
                            smallestYValue = distance;
                    }
                };
            };
        }

        if (smallestYValue === null)
            smallestYValue = 0;
        smallestYValueForPage.push(smallestYValue);
    }

    // Find texts with similar Y values (in the range of Y - smallestYValue to Y + smallestYValue).

    let myPages = [];

    for (let pageIndex = 0; pageIndex < pdf.formImage.Pages.length; pageIndex++) {
        let page = pdf.formImage.Pages[pageIndex];

        let rows = [];  // store texts and their X positions in rows

        for (let textIndex = 0; textIndex < page.Texts.length; textIndex++) {
            let text = page.Texts[textIndex];

            let foundRow = false;
            for (let rowIndex = rows.length - 1; rowIndex >= 0; rowIndex--) {
                // Y value of text falls within the Y value range, add text to row.

                let maximumYdifference = smallestYValueForPage[pageIndex];
                if (rows[rowIndex].y - maximumYdifference < text.y && text.y < rows[rowIndex].y + maximumYdifference) {
                    // Only add value of T to data (which is the actual text).

                    for (let index = 0; index < text.R.length; index++)
                        rows[rowIndex].data.push({ text: decodeURIComponent(text.R[index].T), x: text.x });
                    foundRow = true;
                }
            };

            // Create a new row and add the text to the row.

            if (!foundRow) {
                let row = { y: text.y, data: [] };
                for (let index = 0; index < text.R.length; index++)
                    row.data.push({ text: decodeURIComponent(text.R[index].T), x: text.x });
                rows.push(row);
            }
        };

        // Sort each extracted row horizontally by X co-ordinate.

        for (let index = 0; index < rows.length; index++)
            rows[index].data.sort(xComparer);

        // Sort rows vertically by Y co-ordinate.

        rows.sort(yComparer);

        // Add rows to pages.

        myPages.push(rows);
    };

    // Flatten pages into rows.

    let rows = [];

    for (let pageIndex = 0; pageIndex < myPages.length; pageIndex++) {
        for (let rowIndex = 0; rowIndex < myPages[pageIndex].length; rowIndex++) {
            // Now that each row is made of objects extract the text property from the object.

            let rowEntries = []
            let row = myPages[pageIndex][rowIndex].data;
            for (let index = 0; index < row.length; index++)
                rowEntries.push(row[index].text);

            // Append the extracted and ordered text into the return rows.

            rows.push(rowEntries);
        };
    };

    return rows;
}

main().catch(error => console.error(error));
