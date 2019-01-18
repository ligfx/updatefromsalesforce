import csv
import datetime
import io
import re

def update_from_sfdc(sf, pg, local_tablename, remote_objectname, local_to_remote):
  print("Updating %s..." % local_tablename)

  # result = sf.bulk.Lead.query('select Id, Email, FirstName, LastName, Title, Company, SalesContact, SysModStamp From Lead')

  # print(sf.query('select Id, IsDeleted From Lead where IsDeleted = True', include_deleted=True))
  # exit()

  # TODO:
  # Thinking about overall architecture.
  # - find last updated object locally, then find all remote objects updated after that (currently, also includes that last one.. can we do a gte or is a gt necessary?)
  # - find last deleted object locally, then find all deleted remote objects including and after that. if we don't find that local last deleted object, then.. the remote recycle bin has been emptied and we may be missing deleted records. probably need to do a full re-sync so that we're not missing anything.
  # - can the bulk API retrieve deleted objects?
  # - if we're using the non-bulk query API, we can query for updated and deleted objects at the same time. Get all updated since latest sysmodstamp we have, and get all deleted since the latest deleted sysmodstamp we have. Check that the deleted records includes our last local one, and if we're good then upsert everything into database. Otherwise fall back to a full re-sync.
  # - do we want to fall back to the bulk query API if there are too many records? that would reduce the number of API calls, but is harder to implement.
  # - if we're using the bulk query API for a quick sync (if the query for updated objects has too many results), then we need to check the deleted records in a separate call.

  cur = pg.cursor()

  # TODO: assert no remote field is listed twice

  local_id_colname = None
  local_systemmodstamp_colname = None
  for (local, remote) in local_to_remote:
    if remote.lower() == 'id':
      local_id_colname = local
    if remote.lower() == 'systemmodstamp':
      local_systemmodstamp_colname = local

  if local_id_colname is None:
    raise Exception("Couldn't find local column synced to remote field 'Id'")
  if local_systemmodstamp_colname is None:
    raise Exception("Couldn't find local column synced to remote field 'SystemModstamp'")

  field_transformations = []
  local_columns = []
  remote_fields = []

  cur.execute('select * from %s limit 0' % (local_tablename))
  # must have all columns and in correct order otherwise COPY (and maybe other things) will fail!
  for d in cur.description:
    matching = [_ for _ in local_to_remote if _[0] == d.name]
    if len(matching) != 1:
      raise Exception("Couldn't find local field matching '%s'" % d.name)
    local_columns.append(matching[0][0])
    remote_fields.append(matching[0][1])

  for (local, remote) in local_to_remote:
    if local not in local_columns:
      raise Exception("Field to be synced '%s' doesn't exist" % local)

  # convert timestamps correctly
  for d in cur.description:
    if d.type_code == 1184: # timestamptz
      def f(x):
        if isinstance(x, int):
          return datetime.datetime.utcfromtimestamp(x / 1000).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
          return x
      # field_transformations.append(lambda x: datetime.datetime.utcfromtimestamp(x / 1000).strftime('%Y-%m-%dT%H:%M:%SZ'))
      field_transformations.append(f)
    else:
      field_transformations.append(lambda x: x)

  cur.execute('select count(1) from %s' % local_tablename)
  number_rows_local = cur.fetchone()[0]

  cur.execute('select max(systemmodstamp) from %s' % local_tablename)
  latest_systemmodstamp_local = cur.fetchone()[0]

  print(latest_systemmodstamp_local)

  # Try to get updated results using normal API
  # TODO: Is it safe to do a gt rather than a gte?
  # for f in getattr(sf, remote_objectname).describe()['fields']:
  #   print(f['name'])
  query = (
    'select %s from %s' % (', '.join(remote_fields), remote_objectname)
    + (' where SystemModstamp > %s' % latest_systemmodstamp_local.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if latest_systemmodstamp_local else '')
  )
  print(query)

  result = sf.query(query)
  print("Got %s rows out of %s" % (len(result['records']), result['totalSize']))

  if not result['done'] and result['totalSize'] / len(result['records']) >= 5:
    # Switch to Bulk API for large datasets
    # number_rows_remote = result['totalSize']

    # print("expecting %s rows" % number_rows_remote)
    print("switching to bulk query...")

    number_rows_remote = sf.query('select count() from %s' % remote_objectname)['totalSize']

    # TODO: can we get only new records and insert those?
    # TODO: if you get an IndexError at simple_salesforce/bulk.py line 161,
    # it's probably because of a query error. Run with the non-bulk API to see
    # the actual error.
    records = getattr(sf.bulk, remote_objectname).query("select %s from %s" % (', '.join(remote_fields), remote_objectname))

    if number_rows_remote != len(records):
      raise Exception("expected %s records but got %s. You may have hit a race condition, in which case try again. Otherwise, you're probably encountering a bug in simple_salesforce bulk queries where it only returns the first file." % ( number_rows_remote, len(records)))
    print("got %s rows" % len(records))

    f = io.StringIO()
    w = csv.writer(f)
    w.writerow(local_columns)
    for r in records:
      w.writerow([field_transformations[i](r[fieldname]) for i, fieldname in enumerate(remote_fields)])
    f.seek(0)
    cur.execute('DELETE FROM %s' % local_tablename)
    cur.copy_expert("COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','" % local_tablename, f)
    pg.commit()

  else:
    records = result['records']
    while not result['done']:
      result = sf.query_more(result['nextRecordsUrl'], True)
      records += result['records']

    for record in records:
      command = cur.mogrify(
      """insert into %s (%s)
       values (%s)
       on conflict (id) do update
       set (%s)
         = (%s)
       where %s.id = %s
      """ % (
        local_tablename,
        ', '.join(local_columns),
        ', '.join('%s' for _ in remote_fields),
        ', '.join(local_columns),
        ', '.join('%s' for _ in remote_fields),
        local_tablename,
        '%s'
      ), (
        [record[fieldname] for fieldname in remote_fields]
        + [record[fieldname] for fieldname in remote_fields]
        + [record['Id']]
      ))
      # print(command.decode('utf-8'))
      cur.execute(command)
      pg.commit()

