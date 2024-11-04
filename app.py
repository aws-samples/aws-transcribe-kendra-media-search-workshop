#!/usr/bin/env python3

import aws_cdk as cdk

from media_indexing.media_indexing_stack import MediaIndexingStack


app = cdk.App()
MediaIndexingStack(app, "MediaIndexingStack")


app.synth()
