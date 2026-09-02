// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://tileorm.org',
	integrations: [
		starlight({
			title: 'TileORM',
			description: 'An ORM for Tile38, inspired by Peewee, with Pydantic validation.',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/happycabby-co/tileorm' },
			],
			customCss: ['./src/styles/custom.css'],
			head: [
				{
					tag: 'script',
					attrs: {
						'data-goatcounter': 'https://stats.tileorm.org/count',
						async: true,
						src: '//gc.zgo.at/count.js',
					},
				},
			],
			components: {
				SiteTitle: './src/components/SiteTitle.astro',
			},
			sidebar: [
				{
					label: 'Start here',
					items: [{ label: 'Getting started', slug: 'getting-started' }],
				},
				{
					label: 'Guides',
					items: [
						{ label: 'Defining models', slug: 'guides/models' },
						{ label: 'Querying', slug: 'guides/querying' },
						{ label: 'Saving and deleting', slug: 'guides/instance-methods' },
						{ label: 'Geo types', slug: 'guides/geo-types' },
						{ label: 'Error handling', slug: 'guides/errors' },
					],
				},
				{
					label: 'Reference',
					items: [{ autogenerate: { directory: 'reference' } }],
				},
			],
		}),
	],
});
