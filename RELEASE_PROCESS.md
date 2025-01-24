- Check for breaking changes since the last release: 
	- Make sure your copy of the repository is freshly fetched.
	- `git log v$LAST...` where `$LAST` is, for example `0.5.0`. `git tag` will
		show the list. 
	- `git diff v$LAST... | grep version:` and see if there's any changed lines;
		review to see if they're breaking.
- Decide on a new version number. This is simplified semantic versioning that
	ignores the distinction between feature and bug fix changes.
	- If the major is 0:
		- If the change is breaking, increment the minor and set the patch to 0.
			(`v0.5.1` becomes `v0.6.0`)
		- If the change is not breaking, increment the patch. (`v0.5.1` becomes
			`v0.5.2`)
	- If the major is not 0
		- If the change is breaking, increment the major and set the minor and
			patch to 0. (`v1.5.1` becomes `v2.0.0`)
		- If the change is not breaking, increment the minor and reset the patch to
			0 if it is not. (`v1.5.0` becomes `v1.6.0`)
- Bump the version number in `Cargo.toml`, and run `cargo build` to update
	`Cargo.lock`. Add and commit the version number bump with an appropriate
	commit message, such as `release v1.6.0`. Get the PR merged.
- Make sure the tip of the `main` branch has built with the PR merged.
- [Create a new release](https://github.com/spice-labs-inc/goatrodeo/releases/new).
	- Choose the tip of the `main` branch as the target. 
	- Enter the new version number as the tag, including `v`.
	- Choose the previous tag to base the release notes on. Generate them as
		a starting point and edit as needed. Make sure they look good.
	- Publish the release.
- Make sure the action finished to push the image to DockerHub and that
  the [image on
  DockerHub](https://hub.docker.com/r/spicelabs/goatrodeo/tags) has the
  `v$NEWVERSION` tag.
