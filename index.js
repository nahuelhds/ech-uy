import csv from "csv-parser";
import fs from "fs";
import { Parser } from "@json2csv/plainjs";

normalizeFile(
  "./dict/ECH-2020-PERSONA.csv",
  "./dict/ECH-2020-PERSONA-NORMALIZADO.csv",
  {
    TITLE: "DESCRIPCIÓN DE VARIABLE",
    VARIABLE: "NOMBRE VARIABLE",
    VALUE: "CÓDIGOS",
    LABEL: "DESCRIPCIÓN Y OBSERVACIONES",
  },
);

normalizeFile(
  "./dict/ECH-2020-HOGAR.csv",
  "./dict/ECH-2020-HOGAR-NORMALIZADO.csv",
  {
    TITLE: "DESCRIPCIÓN DE LA VARIABLE",
    VARIABLE: "NOMBRE VARIABLE",
    VALUE: "CÓDIGO",
    LABEL: "DESCRIPCIÓN Y OBSERVACIONES",
  },
);

function normalizeFile(sourceFile, destinationFile, headers) {
  const data = [];
  fs.createReadStream(sourceFile)
    .pipe(csv())
    .on("data", (row) => {
      const previousRow = data[data.length - 1];
      data.push(normalizeRow(row, previousRow, headers));
    })
    .on("end", () => {
      toCSV(
        data.filter((a) => a),
        destinationFile,
      );
    });
}

function normalizeRow(row, previousRow, headers) {
  console.log("Normalizing row", row);

  if (!row) {
    console.warn("Row is empty", row);
    return null;
  }

  if (!previousRow) {
    console.warn("No previous row, nothing to normalize");
    return row;
  }

  if (isGroupTitle(row, headers)) {
    console.warn("The row is a group title, nothing to normalize");
    return row;
  }

  if (isGroupTitle(previousRow, headers)) {
    console.warn("The previous row is a group title, nothing to normalize");
    return row;
  }

  if (!row[headers.TITLE] && previousRow[headers.TITLE]) {
    row[headers.TITLE] = previousRow[headers.TITLE];
  }

  if (!row[headers.VARIABLE] && previousRow[headers.VARIABLE]) {
    row[headers.VARIABLE] = previousRow[headers.VARIABLE];
  }

  return row;
}

function isGroupTitle(row, headers) {
  const filledCols = Object.values(row).filter((col) => col !== "");
  if (filledCols.length !== 1) {
    return false;
  }

  return filledCols[0] === row[headers.TITLE];
}

function toCSV(data, destinationFile) {
  console.log("Normalization ended. Creating CSV...");
  try {
    const parser = new Parser({});
    const csv = parser.parse(data);
    fs.writeFile(destinationFile, csv, function (err) {
      if (err) {
        return console.log(err);
      }
      console.log("The file was successfully created!");
    });
  } catch (err) {
    console.error(err);
  }
}
