const TableStore = require('tablestore')
const Long = TableStore.Long

async function getViewsCount(client) {
    const response = await client.getRow({
        tableName: process.env['TableName'],
        primaryKey: [{ count_name: 'views' }],
        maxVersions: 1,
    })

    return response.row && response.row.primaryKey
        ? response.row.attributes[0].columnValue.toNumber()
        : null
}

exports.handler = function (event, context, callback) {
    (async () => {
        let success = false
        let views = null

        const client = new TableStore.Client({
            accessKeyId: context.credentials.accessKeyId,
            secretAccessKey: context.credentials.accessKeySecret,
            stsToken: context.credentials.securityToken,
            endpoint: `http://${process.env['InstanceName']}.${context.region}.ots.aliyuncs.com`,
            instancename: process.env['InstanceName'],
        })

        do {
            views = await getViewsCount(client) || 0

            const tableName = process.env['TableName']
            const updateOfAttributeColumns = [{ PUT: [{ count: Long.fromNumber(views + 1) }] }]
            const primaryKey = [{ count_name: 'views' }]
            const condition = views
                ? new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, new TableStore.SingleColumnCondition('count', Long.fromNumber(views), TableStore.ComparatorType.EQUAL))
                : new TableStore.Condition(TableStore.RowExistenceExpectation.EXPECT_NOT_EXIST, null)

            try {
                await client.updateRow({
                    tableName,
                    condition,
                    primaryKey,
                    updateOfAttributeColumns,
                })
                success = true
            } catch (ex) {
                console.log(ex)
            }
        } while (!success)

        callback(null, { isBase64Encoded: false, statusCode: 200, body: views })
    })()
}