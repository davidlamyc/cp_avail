import type { NextPage } from "next";
import { useRef } from "react";
import Map, { Layer, MapRef, Marker, Source } from "react-map-gl/maplibre";
import { useEffect, useState } from 'react';
import { ConfigProvider, Breadcrumb, Layout, Menu, theme, Button, Checkbox, Form, Input } from 'antd/lib';
const { Header, Content, Footer } = Layout;

// const locations = [
//   {
//     latitude: 1.3614206826,
//     longitude: 103.8430844601,
//     name: "BLK 246-256 BISHAN STREET 22",
//   },
//   {
//     latitude: 1.3609610362,
//     longitude: 103.8436216081,
//     name: "BLK 246A BISHAN STREET 22",
//   },
// ];

const items = Array.from({ length: 1 }).map((_, index) => ({
  key: String(index + 1),
  label: 'Carpark Availability',
}));

const IndexPage: NextPage = () => {
  // const {
  //   token: { colorBgContainer, borderRadiusLG },
  // } = theme.useToken();
  const [locations, setLocations] = useState([]);
  const [value, setValue] = useState("");

  const mapRef = useRef<MapRef>(null);

  const flyTo = (coordinates: [number, number]): void => {
    const map = mapRef.current?.getMap();
    if (!map) return;

    map.flyTo({
      center: coordinates,
      essential: true,
      zoom: 14,
    });
  };

  useEffect(() => {
    fetch('http://localhost:8000/recommendations', {
      method: "POST",
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        "postal_code": parseInt(value) ? parseInt(value) : 238801,
        "prediction_timestamp": "2025-03-30 01:00:00"
      }),
    })
      .then((response) => response.json())
      .then((data) => {
        setLocations(data);
      });
  }, [value]);

  const onFinish = (values) => {
    console.log('Success:', values);
    setValue(values.postal_code);
  };
  const onFinishFailed = (errorInfo) => {
    console.log('Failed:', errorInfo);
  };

  return (
    <ConfigProvider
      theme={{
        // 1. Use dark algorithm
        algorithm: theme.darkAlgorithm,

        // 2. Combine dark algorithm and compact algorithm
        // algorithm: [theme.darkAlgorithm, theme.compactAlgorithm],
      }}
    >
      <Layout>
        <Header
          style={{
            position: 'sticky',
            top: 0,
            zIndex: 1,
            width: '100%',
            display: 'flex',
            alignItems: 'center',
          }}
        >
          <div className="demo-logo" />
          <Menu
            theme="dark"
            mode="horizontal"
            defaultSelectedKeys={['2']}
            items={items}
            style={{ flex: 1, minWidth: 0 }}
          />
        </Header>
        <Content style={{ padding: '0 48px' }}>
          <Breadcrumb style={{ margin: '16px 0' }}>
            {/* <Breadcrumb.Item>Home</Breadcrumb.Item>
          <Breadcrumb.Item>List</Breadcrumb.Item>
          <Breadcrumb.Item>App</Breadcrumb.Item> */}
          </Breadcrumb>
          <div
            style={{
              padding: 24,
              minHeight: 380,
              // background: colorBgContainer,
              // borderRadius: borderRadiusLG,
            }}
          >
            <Form
              name="basic"
              labelCol={{
                // span: 8,
              }}
              wrapperCol={{
                span: 16,
              }}
              style={{
                maxWidth: 600,
              }}
              initialValues={{
                remember: true,
              }}
              onFinish={onFinish}
              onFinishFailed={onFinishFailed}
              autoComplete="off"
            >
              <Form.Item
                label="Postal Code"
                name="postal_code"
                rules={[
                  {
                    // required: true,
                    message: 'Please input your postal code!',
                  },
                ]}
              >
                <Input />
              </Form.Item>
              <Form.Item label={null}>
                <Button type="primary" htmlType="submit">
                  Submit
                </Button>
              </Form.Item>
            </Form>
            <Map
              ref={mapRef}
              maxBounds={[103.596, 1.1443, 104.1, 1.4835]}
              mapStyle="https://www.onemap.gov.sg/maps/json/raster/mbstyle/Grey.json"
              style={{
                width: "90vw",
                height: "90vh",
              }}
            >
              {locations.map((location) => (
                <Marker
                  key={location.address}
                  latitude={location.latitude}
                  longitude={location.longitude}
                >
                  <div
                    className="mrt-marker"
                    onClick={() => flyTo([location.longitude, location.latitude])}
                  >
                    <p>{location.address}</p>
                    <p>Walking distance: {location.total_distance_in_km}km</p>
                    <p>Predicted availability: {location.predicted_availability} lots</p>
                    <p>Recommendation score: {location.recommendation_score}</p>
                    <style jsx>{`
              .mrt-marker {
                background: red;
                color: white;
                padding: 2px;
              }
            `}</style>
                  </div>
                </Marker>
              ))}
            </Map>
          </div>
        </Content>
        <Footer style={{ textAlign: 'center' }}>
          Ant Design ©{new Date().getFullYear()} Created by Ant UED
        </Footer>
      </Layout>
    </ConfigProvider>
  );
};

export default IndexPage;