put_new_value

Is it possible preload on a joined association and then filter on the preloaded association?
Is it possible preload on a joined association and then use select on the preloaded association?

Is there a test that verifies what happens if i join on an association,
with :preload (which should delegate to Ecto.Query.preload) and then apply
a filter on the preloaded values?

review the implementation of batch_preload and Ecto.Query.preload. Tell me if this approach of batching
the query request offers any benefit to using Ecto.Query.preload for bulk operations like insert_all. It
doesn't support nested preloading at the moment, right now we're focused on figuring out if it's worth
keeping or if we should remove it and use preload/3. 

1. move the tests to the correct file now that we're done reconciling the behaviour. Also revise the comments and descriptions. The conversation shouldnt leak into the description and comments shouldnt leak


- Support for schemaless operations
- all of filters have a strign values like "lock" should be reviewed
- look for String.to_existing_atom