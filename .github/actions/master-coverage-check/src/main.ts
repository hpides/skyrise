import * as core from '@actions/core'
import * as aws from '@aws-sdk/client-s3'
import * as fs from 'fs';
import { Readable } from 'stream';

async function streamToString(stream: Readable): Promise<string> {
    const chunks: Buffer[] = [];
    return new Promise<string>((resolve, reject) => {
        stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
        stream.on('end', () => resolve(Buffer.concat(chunks).toString()));
        stream.on('error', (error) => reject(error));
    });
}

function extractBucketAndKey(remoteObject: string): aws.GetObjectCommandInput {
    const parts = remoteObject.split("/");
    const bucket = parts[0];
    const key = parts.slice(1).join('/');
    
    return {
        'Bucket': bucket,
        'Key': key
    };
}

async function commitCoverage(client: aws.S3Client, localCoverage: string, remoteObject: string): Promise<void> {
    const params = extractBucketAndKey(remoteObject);
    params["Body"] = localCoverage;
    await client.send(new aws.PutObjectCommand(params));
}

async function compareCoverageCheck(client: aws.S3Client, localCoverage: string, remoteObject: string): Promise<void> {
    const result = await client.send(new aws.GetObjectCommand(extractBucketAndKey(remoteObject)));
    if(!result.Body) {
        throw new Error("Could not read current coverage file from S3.");
    }
    
    const remoteCoverage = await streamToString(result.Body as Readable);
    
    const localCoverageFloat = parseFloat(localCoverage);
    const remoteCoverageFloat = parseFloat(remoteCoverage);

    const coverage_difference_sign = (localCoverageFloat >= remoteCoverageFloat ? '+' : '-');
    const coverage_difference_text = (localCoverageFloat - remoteCoverageFloat).toFixed(2);
    const message = `This branch has a test code coverage of ${localCoverage} (${coverage_difference_sign}${coverage_difference_text}% against the Master's ${remoteCoverage}).`;

    if(localCoverageFloat < remoteCoverageFloat) {
        core.setFailed(message);
    } else {
        core.info(message);
    }
}

async function run(): Promise<void> {
    const coverageFileName = core.getInput('coverage-file-name');
    const remoteObject = core.getInput('s3-object-name');
    const mode = core.getInput('mode');
    const region = core.getInput('region');
    
    const coverageString = fs.readFileSync(coverageFileName).toString().trim();
    const s3Client = new aws.S3Client({'region': region});

    if(mode == "compare") {
        await compareCoverageCheck(s3Client, coverageString, remoteObject);
    } else if(mode == "commit") {
        await commitCoverage(s3Client, coverageString, remoteObject);
    } else {
        core.setFailed('Invalid value for parameter "mode". Found ' + mode);
    }
}

// Calling run
run().catch(error => core.setFailed(error.message));
